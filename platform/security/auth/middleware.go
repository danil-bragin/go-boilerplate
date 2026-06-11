package auth

import (
	"errors"
	"net/http"
	"strings"

	"go-boilerplate/platform/apperr"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/web/httpx"
)

// DefaultMaxTokenBytes is the default Bearer-token size cap applied by
// Middleware when no WithMaxTokenBytes option is given. 8 KiB comfortably
// holds a fat Keycloak access token (many roles + custom claims) while keeping
// an attacker from forcing expensive jwt.Parse work with a multi-megabyte
// Authorization header.
const DefaultMaxTokenBytes = 8192

// middlewareOptions holds the configurable knobs for Middleware.
type middlewareOptions struct {
	maxTokenBytes int
}

// MiddlewareOption configures Middleware.
type MiddlewareOption func(*middlewareOptions)

// WithMaxTokenBytes overrides the Bearer-token size cap (AUTH_MAX_TOKEN_BYTES).
// A non-positive value falls back to DefaultMaxTokenBytes, so a misconfigured
// or unset env knob can never disable the guard.
func WithMaxTokenBytes(n int) MiddlewareOption {
	return func(o *middlewareOptions) {
		if n > 0 {
			o.maxTokenBytes = n
		}
	}
}

// Middleware returns an HTTP middleware that reads a Bearer token from the
// Authorization header, verifies it with v, and stores the resulting Principal
// in the request context.
//
// If the header is missing or malformed, the token exceeds the size cap
// (WithMaxTokenBytes, default DefaultMaxTokenBytes), or verification fails, it
// responds with 401 application/problem+json and does not call next.
//
// The size cap is checked BEFORE Verify (and thus before jwt.Parse), so an
// attacker cannot force per-request signature/parse work with an oversized
// Authorization header — the rejection is a single len() comparison.
func Middleware(v Verifier, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	o := middlewareOptions{maxTokenBytes: DefaultMaxTokenBytes}
	for _, opt := range opts {
		opt(&o)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := bearerToken(r)
			if !ok {
				httpx.WriteError(w, r, apperr.New(apperr.CodeAuthUnauthenticated).
					WithParam("reason", "missing or malformed Authorization header"))
				return
			}

			// Size cap BEFORE Verify: reject oversized tokens with a cheap
			// len-compare so a giant Authorization header cannot force
			// expensive jwt.Parse / signature-verification work.
			if len(raw) > o.maxTokenBytes {
				httpx.WriteError(w, r, apperr.New(apperr.CodeAuthUnauthenticated).
					WithParam("reason", "token exceeds maximum size"))
				return
			}

			p, err := v.Verify(r.Context(), raw)
			if err != nil {
				// SECURITY: never echo verifier internals (JWKS URLs, network
				// errors, …) to the client. Token-validation failures get the
				// generic "invalid token"; anything else (infrastructure
				// failure) gets an even more generic detail and the real
				// error goes to the structured log only.
				reason := "invalid token"
				if !errors.Is(err, ErrInvalidToken) {
					reason = "authentication failed"
					log.From(r.Context()).ErrorContext(
						r.Context(),
						"auth: token verification failed with non-token error",
						"error", err,
					)
				}
				httpx.WriteError(w, r, apperr.New(apperr.CodeAuthUnauthenticated).
					WithParam("reason", reason))
				return
			}

			next.ServeHTTP(w, r.WithContext(Into(r.Context(), p)))
		})
	}
}

// RequireRole returns a middleware that requires the authenticated Principal
// (stored in ctx by Middleware) to have the given role.  If the principal is
// absent it responds 401; if the role is missing it responds 403.
func RequireRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			p, ok := From(req.Context())
			if !ok {
				httpx.WriteError(w, req, apperr.New(apperr.CodeAuthUnauthenticated).
					WithParam("reason", "no authenticated principal"))
				return
			}

			for _, r := range p.Roles {
				if r == role {
					next.ServeHTTP(w, req)
					return
				}
			}

			httpx.WriteError(w, req, apperr.New(apperr.CodeAuthForbidden).
				WithParam("required_role", role))
		})
	}
}

// bearerToken extracts the raw JWT from an "Authorization: Bearer <token>"
// header. It returns ("", false) on any failure.
func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	tok := strings.TrimPrefix(h, prefix)
	if tok == "" {
		return "", false
	}
	return tok, true
}
