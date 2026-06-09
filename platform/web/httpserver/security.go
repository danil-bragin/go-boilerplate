package httpserver

import (
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/time/rate"
)

// SecurityHeaders sets defensive HTTP response headers on every response.
//
// Headers applied:
//   - X-Content-Type-Options: nosniff   — prevents MIME-sniffing attacks.
//   - X-Frame-Options: DENY             — prevents clickjacking.
//   - Referrer-Policy: no-referrer      — suppresses Referer leakage.
//   - Content-Security-Policy: default-src 'none' — restrictive policy for
//     pure API responses (no HTML); adjust for HTML-serving endpoints.
//   - X-XSS-Protection: 0               — disabled per OWASP guidance; modern
//     browsers use CSP instead, and the legacy XSS filter can itself be abused.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'")
		h.Set("X-XSS-Protection", "0")
		next.ServeHTTP(w, r)
	})
}

// CORSOptions configures the CORS middleware.
type CORSOptions struct {
	// AllowedOrigins is the list of origins that may make cross-origin requests.
	// Use []string{"*"} to allow any origin (not recommended for credentialed
	// requests). Defaults to an empty list (no origin allowed).
	AllowedOrigins []string

	// AllowedMethods is the list of HTTP methods allowed for cross-origin requests.
	// Defaults to GET, POST, PUT, PATCH, DELETE, OPTIONS.
	AllowedMethods []string

	// AllowedHeaders is the list of request headers allowed for cross-origin
	// requests. Defaults to Content-Type, Authorization.
	AllowedHeaders []string

	// MaxAge is the value in seconds for the Access-Control-Max-Age preflight
	// cache. Defaults to 300 (5 minutes) if zero.
	MaxAge int
}

// CORS returns a minimal, hand-rolled CORS middleware (no heavy dependency).
//
// It handles preflight OPTIONS requests by writing the appropriate
// Access-Control-Allow-* headers and returning 204. For actual requests it
// appends the Access-Control-Allow-Origin header when the Origin matches an
// allowed origin.
//
// For production use with credentials (cookies / Authorization headers) set
// AllowedOrigins to the exact allowed origin(s); never use "*" with
// credentialed requests.
func CORS(opts CORSOptions) func(http.Handler) http.Handler {
	methods := opts.AllowedMethods
	if len(methods) == 0 {
		methods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	}
	headers := opts.AllowedHeaders
	if len(headers) == 0 {
		headers = []string{"Content-Type", "Authorization"}
	}
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = 300
	}

	methodsStr := strings.Join(methods, ", ")
	headersStr := strings.Join(headers, ", ")
	maxAgeStr := strconv.Itoa(maxAge)

	// Build a set for O(1) origin lookup.
	originSet := make(map[string]bool, len(opts.AllowedOrigins))
	allowAll := false
	for _, o := range opts.AllowedOrigins {
		if o == "*" {
			allowAll = true
		}
		originSet[o] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Not a CORS request.
				next.ServeHTTP(w, r)
				return
			}

			allowed := allowAll || originSet[origin]

			if r.Method == http.MethodOptions {
				// Preflight request.
				if allowed {
					h := w.Header()
					h.Set("Access-Control-Allow-Origin", origin)
					h.Set("Access-Control-Allow-Methods", methodsStr)
					h.Set("Access-Control-Allow-Headers", headersStr)
					h.Set("Access-Control-Max-Age", maxAgeStr)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}

			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit returns a global token-bucket rate-limiter middleware.
//
// Every request consumes one token. When the bucket is exhausted (burst
// depleted and refill rate exceeded) the middleware returns 429 Too Many
// Requests immediately without calling the next handler.
//
// Parameters:
//   - rps:   sustained requests per second (token refill rate).
//   - burst: maximum burst size (token bucket capacity).
//
// NOTE: This is a single global limiter shared across all clients. For
// production deployments, replace with a per-IP limiter (map[string]*rate.Limiter
// keyed on r.RemoteAddr or the X-Forwarded-For header) with an LRU eviction
// policy to prevent memory exhaustion. A Redis-backed distributed rate limiter
// (e.g., GCRA via redis/rueidis) is the correct choice for multi-instance
// deployments.
func RateLimit(rps float64, burst int) func(http.Handler) http.Handler {
	limiter := rate.NewLimiter(rate.Limit(rps), burst)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
