package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
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

// WithClockSkew sets the acceptable clock skew applied to exp/iat/nbf
// validation (jwt.WithAcceptableSkew). Distributed issuers and verifiers
// rarely share a perfectly synchronized clock; a small skew (e.g. 30s,
// AUTH_CLOCK_SKEW) prevents spurious rejections of freshly-issued tokens
// whose iat/nbf is marginally in the future. Default: 0 (strict).
func WithClockSkew(d time.Duration) Option {
	return func(v *JWKSVerifier) {
		v.clockSkew = d
	}
}

// WithRequiredAZP requires the token's "azp" (authorized party) claim to equal
// azp (AUTH_REQUIRED_AZP). Use this when several clients share one realm and
// audience: it pins tokens to the specific OAuth client they were issued to,
// so a token minted for another client cannot be replayed against this
// service. Empty string (default) disables the check.
func WithRequiredAZP(azp string) Option {
	return func(v *JWKSVerifier) {
		v.requiredAZP = azp
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
	clockSkew      time.Duration
	requiredAZP    string
}

// jwksInitTimeout bounds the initial JWKS fetch in NewJWKSVerifier. jwx v3's
// cache.Register blocks until the resource is ready; without this bound an
// unreachable IdP would hang service startup forever instead of failing
// closed with an error.
const jwksInitTimeout = 10 * time.Second

// NewJWKSVerifier creates a JWKSVerifier that fetches the JWKS from jwksURL
// and validates the given issuer and audience on every token.
//
// ctx is used to drive the background cache refresh goroutine; it should be
// the application lifetime context. The INITIAL fetch is additionally bounded
// by jwksInitTimeout so a misconfigured or unreachable JWKS URL surfaces as a
// startup error (fail closed), never a hang.
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

	if jwksURL == "" {
		return nil, fmt.Errorf("%w: JWKS URL must not be empty", ErrInvalidToken)
	}

	cache, err := jwk.NewCache(ctx, httprc.NewClient())
	if err != nil {
		return nil, fmt.Errorf("auth: creating JWKS cache: %w", err)
	}

	// Register blocks until the first fetch succeeds (or initCtx expires) —
	// bound it so an unreachable IdP fails startup instead of hanging it.
	initCtx, cancel := context.WithTimeout(ctx, jwksInitTimeout)
	defer cancel()
	if err := cache.Register(initCtx, jwksURL); err != nil {
		return nil, fmt.Errorf("auth: registering JWKS URL: %w", err)
	}

	// Confirm the keyset is readable so any configuration error is caught early.
	if _, err := cache.Lookup(initCtx, jwksURL); err != nil {
		return nil, fmt.Errorf("auth: initial JWKS fetch from %s: %w", jwksURL, err)
	}

	v.cache = cache
	return v, nil
}

// Verify parses and validates rawToken, returning a populated Principal on
// success. Token-validation failures (bad signature, expired, wrong
// issuer/audience, azp mismatch) return a wrapped ErrInvalidToken.
//
// Infrastructure failures — the JWKS cache lookup failing because the IdP is
// unreachable — are deliberately NOT wrapped in ErrInvalidToken: the HTTP
// middleware logs non-token errors, so an IdP outage surfaces in logs instead
// of becoming a silent storm of generic 401s.
func (v *JWKSVerifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	keyset, err := v.cache.Lookup(ctx, v.jwksURL)
	if err != nil {
		return Principal{}, fmt.Errorf("auth: fetching JWKS: %w", err)
	}

	tok, err := jwt.Parse(
		[]byte(rawToken),
		jwt.WithKeySet(keyset),
		jwt.WithValidate(true),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithRequiredClaim("exp"),
		jwt.WithAcceptableSkew(v.clockSkew),
	)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	// Authorized-party pinning: when configured, the azp claim must match.
	if v.requiredAZP != "" {
		var azp string
		if err := tok.Get("azp", &azp); err != nil || azp != v.requiredAZP {
			return Principal{}, fmt.Errorf("%w: azp claim mismatch", ErrInvalidToken)
		}
	}

	// The subject is the identity the authz + idempotency keyspace is keyed on.
	// Reject an empty/absent sub: admitting it would collapse every actor into a
	// shared global namespace (cross-actor idempotency-key collisions, an
	// ownership-check hole). Fail closed.
	sub, _ := tok.Subject()
	if sub == "" {
		return Principal{}, fmt.Errorf("%w: token has empty subject claim", ErrInvalidToken)
	}
	p := Principal{
		Subject: sub,
		Claims:  privateClaims(tok),
	}

	// Extract preferred_username if present.
	var username string
	if err := tok.Get("preferred_username", &username); err == nil {
		p.Username = username
	}

	// Extract roles by walking the dot-separated claim path.
	p.Roles = v.extractRoles(tok)

	return p, nil
}

// standardClaims are the registered JWT claims excluded from Principal.Claims
// (which mirrors jwx v2's PrivateClaims behaviour).
var standardClaims = map[string]struct{}{
	"iss": {}, "sub": {}, "aud": {}, "exp": {}, "nbf": {}, "iat": {}, "jti": {},
}

// privateClaims collects all non-registered claims from tok into a map,
// mirroring the v2 PrivateClaims() accessor that v3 removed.
func privateClaims(tok jwt.Token) map[string]any {
	claims := make(map[string]any)
	for _, name := range tok.Keys() {
		if _, std := standardClaims[name]; std {
			continue
		}
		var val any
		if err := tok.Get(name, &val); err == nil {
			claims[name] = val
		}
	}
	return claims
}

// extractRoles walks the configured rolesClaimPath in the token's private
// claims and returns the resulting string slice.  The path is dot-separated;
// e.g. "realm_access.roles" navigates into map["realm_access"]["roles"].
func (v *JWKSVerifier) extractRoles(tok jwt.Token) []string {
	parts := strings.Split(v.rolesClaimPath, ".")
	if len(parts) == 0 {
		return nil
	}

	// Fetch the first segment from the token, then walk nested maps.
	var current any
	if err := tok.Get(parts[0], &current); err != nil {
		return nil
	}

	for _, part := range parts[1:] {
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
