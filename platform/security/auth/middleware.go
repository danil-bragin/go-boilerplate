package auth

import (
	"errors"
	"net/http"
	"strings"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/web/httpx"
)

// Middleware returns an HTTP middleware that reads a Bearer token from the
// Authorization header, verifies it with v, and stores the resulting Principal
// in the request context.
//
// If the header is missing or malformed, or verification fails, it responds
// with 401 application/problem+json and does not call next.
func Middleware(v Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := bearerToken(r)
			if !ok {
				httpx.WriteProblem(w, httpx.Problem{
					Status: http.StatusUnauthorized,
					Title:  "Unauthorized",
					Detail: "missing or malformed Authorization header",
				})
				return
			}

			p, err := v.Verify(r.Context(), raw)
			if err != nil {
				// SECURITY: never echo verifier internals (JWKS URLs, network
				// errors, …) to the client. Token-validation failures get the
				// generic "invalid token"; anything else (infrastructure
				// failure) gets an even more generic detail and the real
				// error goes to the structured log only.
				detail := "invalid token"
				if !errors.Is(err, ErrInvalidToken) {
					detail = "authentication failed"
					log.From(r.Context()).ErrorContext(r.Context(),
						"auth: token verification failed with non-token error",
						"error", err,
					)
				}
				httpx.WriteProblem(w, httpx.Problem{
					Status: http.StatusUnauthorized,
					Title:  "Unauthorized",
					Detail: detail,
				})
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
				httpx.WriteProblem(w, httpx.Problem{
					Status: http.StatusUnauthorized,
					Title:  "Unauthorized",
					Detail: "no authenticated principal",
				})
				return
			}

			for _, r := range p.Roles {
				if r == role {
					next.ServeHTTP(w, req)
					return
				}
			}

			httpx.WriteProblem(w, httpx.Problem{
				Status: http.StatusForbidden,
				Title:  "Forbidden",
				Detail: "required role: " + role,
			})
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
