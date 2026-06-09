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
	assert.Equal(t, "user,admin", headers[auth.HeaderPrincipalRoles])
	assert.Equal(t, "m1", headers["message-id"], "existing headers must be preserved")

	outCtx := auth.ExtractToContext(context.Background(), headers)
	p, ok := auth.From(outCtx)
	require.True(t, ok, "principal must be installed in ctx")
	assert.Equal(t, "user-123", p.Subject)
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
