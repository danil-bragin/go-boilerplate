package auth_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/security/auth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrincipalHeaders_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := auth.Into(context.Background(), auth.Principal{
		Subject: "user-123",
		Roles:   []string{"user", "admin"},
	})

	headers := map[string]string{"message-id": "m1"}
	auth.InjectHeaders(ctx, headers)

	assert.Equal(t, "user-123", headers[auth.HeaderPrincipalSub])
	assert.Equal(t, `["user","admin"]`, headers[auth.HeaderPrincipalRoles],
		"roles travel as a JSON array (lossless for roles containing commas)")
	assert.Equal(t, "m1", headers["message-id"], "existing headers must be preserved")

	outCtx := auth.ExtractToContext(context.Background(), headers)
	p, ok := auth.From(outCtx)
	require.True(t, ok, "principal must be installed in ctx")
	assert.Equal(t, "user-123", p.Subject)
	assert.Equal(t, []string{"user", "admin"}, p.Roles)
}

// TestPrincipalHeaders_RoleWithComma_RoundTripsExactly: a role containing a
// comma must survive inject→extract unchanged — the old comma-join/split
// encoding synthesized phantom roles at the consumer.
func TestPrincipalHeaders_RoleWithComma_RoundTripsExactly(t *testing.T) {
	t.Parallel()

	roles := []string{"tenant:a,b", "admin", " spaced "}
	ctx := auth.Into(context.Background(), auth.Principal{
		Subject: "user-9",
		Roles:   roles,
	})

	headers := map[string]string{}
	auth.InjectHeaders(ctx, headers)

	p, ok := auth.From(auth.ExtractToContext(context.Background(), headers))
	require.True(t, ok)
	assert.Equal(t, roles, p.Roles, "roles must round-trip losslessly, commas and spaces included")
}

// TestExtractToContext_LegacyCommaJoinedRoles: records produced by an older
// service version carry comma-joined roles — the extractor must still read
// them (mixed-version rollout safety).
func TestExtractToContext_LegacyCommaJoinedRoles(t *testing.T) {
	t.Parallel()

	ctx := auth.ExtractToContext(context.Background(), map[string]string{
		auth.HeaderPrincipalSub:   "svc-legacy",
		auth.HeaderPrincipalRoles: "user, admin",
	})
	p, ok := auth.From(ctx)
	require.True(t, ok)
	assert.Equal(t, []string{"user", "admin"}, p.Roles)
}

func TestInjectHeaders_NoPrincipalNoop(t *testing.T) {
	t.Parallel()

	headers := map[string]string{}
	auth.InjectHeaders(context.Background(), headers)
	assert.Empty(t, headers, "no principal in ctx must add no headers")
}

func TestExtractToContext_NoHeadersNoop(t *testing.T) {
	t.Parallel()

	ctx := auth.ExtractToContext(context.Background(), map[string]string{"x": "y"})
	_, ok := auth.From(ctx)
	assert.False(t, ok, "no principal headers must not install a principal")

	ctx = auth.ExtractToContext(context.Background(), nil)
	_, ok = auth.From(ctx)
	assert.False(t, ok)
}

func TestExtractToContext_EmptyRoles(t *testing.T) {
	t.Parallel()

	ctx := auth.ExtractToContext(context.Background(), map[string]string{
		auth.HeaderPrincipalSub: "svc-1",
	})
	p, ok := auth.From(ctx)
	require.True(t, ok)
	assert.Equal(t, "svc-1", p.Subject)
	assert.Empty(t, p.Roles)
}
