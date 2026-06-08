package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// Option configures a JWKSVerifier.
type Option func(*JWKSVerifier)

// WithRolesClaimPath sets the dot-separated claim path from which roles are
// extracted (default: "realm_access.roles", Keycloak convention).
func WithRolesClaimPath(path string) Option {
	return func(v *JWKSVerifier) {
		v.rolesClaimPath = path
	}
}

// JWKSVerifier verifies RS256 JWTs by fetching and caching the public JWKS
// from a remote URL. The JWKS is refreshed automatically in the background.
type JWKSVerifier struct {
	cache          *jwk.Cache
	jwksURL        string
	issuer         string
	audience       string
	rolesClaimPath string
}

// NewJWKSVerifier creates a JWKSVerifier that fetches the JWKS from jwksURL
// and validates the given issuer and audience on every token.
//
// ctx is used to drive the background cache refresh goroutine; it should be
// the application lifetime context.
//
// JWKS keys MUST carry an "alg" field (e.g. "alg":"RS256"). The verifier pins
// the algorithm from the key, not from the incoming token header — this is the
// alg-confusion defence. A key without "alg" will cause valid tokens to fail
// closed (rejected), which is intentional.
func NewJWKSVerifier(ctx context.Context, jwksURL, issuer, audience string, opts ...Option) (*JWKSVerifier, error) {
	v := &JWKSVerifier{
		jwksURL:        jwksURL,
		issuer:         issuer,
		audience:       audience,
		rolesClaimPath: "realm_access.roles",
	}
	for _, o := range opts {
		o(v)
	}

	cache := jwk.NewCache(ctx)
	if err := cache.Register(jwksURL); err != nil {
		return nil, fmt.Errorf("auth: registering JWKS URL: %w", err)
	}

	// Perform an initial fetch so that any configuration error is caught early.
	if _, err := cache.Refresh(ctx, jwksURL); err != nil {
		return nil, fmt.Errorf("auth: initial JWKS fetch from %s: %w", jwksURL, err)
	}

	v.cache = cache
	return v, nil
}

// Verify parses and validates rawToken, returning a populated Principal on
// success. On any validation failure it returns a wrapped ErrInvalidToken.
func (v *JWKSVerifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	keyset, err := v.cache.Get(ctx, v.jwksURL)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: fetching JWKS: %w", ErrInvalidToken, err)
	}

	tok, err := jwt.Parse(
		[]byte(rawToken),
		jwt.WithKeySet(keyset),
		jwt.WithValidate(true),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithRequiredClaim(jwt.ExpirationKey),
	)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	p := Principal{
		Subject: tok.Subject(),
		Claims:  tok.PrivateClaims(),
	}

	// Extract preferred_username if present.
	if u, ok := tok.Get("preferred_username"); ok {
		if s, ok := u.(string); ok {
			p.Username = s
		}
	}

	// Extract roles by walking the dot-separated claim path.
	p.Roles = v.extractRoles(tok)

	return p, nil
}

// extractRoles walks the configured rolesClaimPath in the token's private
// claims and returns the resulting string slice.  The path is dot-separated;
// e.g. "realm_access.roles" navigates into map["realm_access"]["roles"].
func (v *JWKSVerifier) extractRoles(tok jwt.Token) []string {
	parts := strings.Split(v.rolesClaimPath, ".")
	if len(parts) == 0 {
		return nil
	}

	// Start from private claims for the first segment.
	var current any = tok.PrivateClaims()

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[part]
	}

	// The leaf value must be a []interface{} of strings.
	items, ok := current.([]any)
	if !ok {
		return nil
	}

	roles := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			roles = append(roles, s)
		}
	}
	return roles
}
