package tenant

import (
	"context"
	"fmt"
	"net/http"

	"go-boilerplate/platform/security/auth"
)

// DefaultClaim is the JWT claim name Middleware reads when none is configured.
const DefaultClaim = "tenant"

// Middleware returns an HTTP middleware that resolves the tenant id from the
// authenticated principal's claims (the claim named by claim, or DefaultClaim
// when claim is empty) and installs it into the request context via
// WithContext.
//
// It MUST be chained AFTER auth.Middleware, which puts the verified principal
// in the context. Resolution is best-effort: if there is no principal, no such
// claim, or the claim is empty, the request proceeds unchanged with no tenant
// in context. Enforcement (fail-closed) is the job of the Require CQRS
// behavior, mirroring how auth.Middleware authenticates while authz.Require
// authorizes.
//
// The tenant value comes from the cryptographically verified JWT, not from a
// client-supplied header, so the value installed here is trustworthy.
func Middleware(claim string) func(http.Handler) http.Handler {
	if claim == "" {
		claim = DefaultClaim
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if id := claimValue(ctx, claim); id != "" {
				ctx = WithContext(ctx, id)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// claimValue extracts the named claim from the ctx principal as a string. It
// returns "" when there is no principal, the claim is absent, or the value is
// an empty string. Non-string claim values are rendered with fmt so numeric
// tenant ids still resolve.
func claimValue(ctx context.Context, claim string) string {
	p, ok := auth.From(ctx)
	if !ok {
		return ""
	}
	v, ok := p.Claims[claim]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
