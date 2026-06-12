package tenant_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/security/tenant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithContext_FromContext_RoundTrip(t *testing.T) {
	t.Parallel()

	id, ok := tenant.FromContext(context.Background())
	assert.False(t, ok, "empty ctx carries no tenant")
	assert.Empty(t, id)

	ctx := tenant.WithContext(context.Background(), "acme")
	id, ok = tenant.FromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "acme", id)
}

func TestWithContext_EmptyIDIsNotStored(t *testing.T) {
	t.Parallel()

	ctx := tenant.WithContext(context.Background(), "")
	_, ok := tenant.FromContext(ctx)
	assert.False(t, ok, "empty id must be treated as no-tenant")
}

func TestHeaders_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := tenant.WithContext(context.Background(), "acme")
	headers := map[string]string{"message-id": "m1"}
	tenant.InjectHeaders(ctx, headers)

	assert.Equal(t, "acme", headers[tenant.HeaderTenantID])
	assert.Equal(t, "m1", headers["message-id"], "existing headers preserved")

	out := tenant.ExtractToContext(context.Background(), headers)
	id, ok := tenant.FromContext(out)
	require.True(t, ok)
	assert.Equal(t, "acme", id)
}

func TestInjectHeaders_NoTenant_NoOp(t *testing.T) {
	t.Parallel()

	headers := map[string]string{}
	tenant.InjectHeaders(context.Background(), headers)
	_, present := headers[tenant.HeaderTenantID]
	assert.False(t, present, "no tenant in ctx → header not written")
}

func TestExtractToContext_AbsentHeader_Unchanged(t *testing.T) {
	t.Parallel()

	out := tenant.ExtractToContext(context.Background(), map[string]string{})
	_, ok := tenant.FromContext(out)
	assert.False(t, ok)
}

func TestMiddleware_ResolvesTenantFromClaim(t *testing.T) {
	t.Parallel()

	var got string
	var gotOK bool
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, gotOK = tenant.FromContext(r.Context())
	})

	h := tenant.Middleware("")(next) // "" → DefaultClaim
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req = req.WithContext(auth.Into(req.Context(), auth.Principal{
		Subject: "u1",
		Claims:  map[string]any{tenant.DefaultClaim: "acme"},
	}))
	h.ServeHTTP(httptest.NewRecorder(), req)

	require.True(t, gotOK)
	assert.Equal(t, "acme", got)
}

func TestMiddleware_CustomClaim_AndNumericValue(t *testing.T) {
	t.Parallel()

	var got string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = tenant.FromContext(r.Context())
	})

	h := tenant.Middleware("org_id")(next)
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req = req.WithContext(auth.Into(req.Context(), auth.Principal{
		Claims: map[string]any{"org_id": 42},
	}))
	h.ServeHTTP(httptest.NewRecorder(), req)

	assert.Equal(t, "42", got, "numeric claim rendered as string")
}

func TestMiddleware_NoPrincipal_PassesThrough(t *testing.T) {
	t.Parallel()

	var ok bool
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, ok = tenant.FromContext(r.Context())
	})

	h := tenant.Middleware("")(next)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	assert.False(t, ok, "no principal → no tenant, request still proceeds")
}

func TestRequire_FailsClosedWithoutTenant(t *testing.T) {
	t.Parallel()

	called := false
	h := tenant.Require[string, string]()(func(_ context.Context, _ string) (string, error) {
		called = true
		return "ok", nil
	})

	_, err := h(context.Background(), "cmd")
	require.Error(t, err)
	assert.Equal(t, apperr.CodeTenantRequired, apperr.Code(err))
	assert.False(t, called, "handler must not run without a tenant")
	assert.ErrorIs(t, err, tenant.ErrRequired)
}

func TestRequire_PassesWithTenant(t *testing.T) {
	t.Parallel()

	h := tenant.Require[string, string]()(func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	})

	out, err := h(tenant.WithContext(context.Background(), "acme"), "cmd")
	require.NoError(t, err)
	assert.Equal(t, "ok", out)
}

// compile-time assertion that Require yields a cqrs.Behavior.
var _ cqrs.Behavior[string, string] = tenant.Require[string, string]()
