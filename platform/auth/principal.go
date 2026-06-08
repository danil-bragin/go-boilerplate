// Package auth provides OIDC/JWT verification via JWKS and pluggable HTTP
// middleware for authenticating requests.
package auth

import "context"

// Principal represents an authenticated identity extracted from a JWT.
type Principal struct {
	Subject  string
	Username string
	Roles    []string
	Claims   map[string]any
}

type ctxKey struct{}

// Into stores the Principal in ctx and returns the new context.
func Into(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// From retrieves the Principal from ctx. The second return value is false if
// no Principal has been stored.
func From(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
