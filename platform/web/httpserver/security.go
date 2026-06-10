package httpserver

import (
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"go-boilerplate/platform/web/httpx"
	"go-boilerplate/platform/web/ratelimit"

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
	// Empty (the default) means DENY ALL: no Access-Control-Allow-Origin header
	// is ever emitted and preflights are rejected with 403. Use []string{"*"}
	// to allow any origin (dev only — never with credentialed requests).
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
// Behaviour:
//   - Deny-by-default: with no AllowedOrigins configured, no
//     Access-Control-Allow-* header is ever emitted.
//   - A preflight is an OPTIONS request carrying Access-Control-Request-Method
//     (the browser always sends it); a plain OPTIONS request without that
//     header is an ordinary request and reaches the handler.
//   - Allowed preflight → 204 with the Access-Control-Allow-* headers.
//   - Disallowed preflight → 403 problem+json with NO CORS headers.
//   - Actual requests from an allowed origin get Access-Control-Allow-Origin;
//     disallowed origins get none (the browser blocks the response).
//   - Vary: Origin is set UNCONDITIONALLY — also on responses to requests
//     without an Origin header — so shared caches always key on Origin and
//     never serve a response with one origin's CORS headers to another
//     origin (or a header-less response to a CORS request).
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
		if o == "" {
			continue // tolerate empty entries from env parsing ("" default = deny-all)
		}
		if o == "*" {
			allowAll = true
		}
		originSet[o] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The response varies with the Origin request header: caches must
			// key on it for EVERY response from this endpoint, including
			// responses to requests without an Origin header (otherwise a
			// cached header-less response could be served to a CORS request).
			w.Header().Add("Vary", "Origin")

			origin := r.Header.Get("Origin")
			if origin == "" {
				// Not a CORS request.
				next.ServeHTTP(w, r)
				return
			}

			allowed := allowAll || originSet[origin]

			// A real preflight carries Access-Control-Request-Method; a plain
			// OPTIONS request without it is an ordinary request.
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				// Preflight request.
				if !allowed {
					// No CORS headers for disallowed origins; reject outright.
					httpx.WriteProblem(w, httpx.Problem{
						Status: http.StatusForbidden,
						Title:  "Forbidden",
						Detail: "origin not allowed",
					})
					return
				}
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", origin)
				h.Set("Access-Control-Allow-Methods", methodsStr)
				h.Set("Access-Control-Allow-Headers", headersStr)
				h.Set("Access-Control-Max-Age", maxAgeStr)
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
// Deprecated: use RateLimitPer with ClientIPKey — a global bucket lets one client starve all.
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

// ClientIPKey returns a key function that extracts the caller's real IP address
// from a request. RemoteAddr is authoritative unless it belongs to a trusted
// proxy prefix, in which case X-Forwarded-For is walked right-to-left and the
// first hop NOT in trusted is used (the closest untrusted client). XFF from
// untrusted peers is ignored — it is trivially spoofable.
//
// When trusted is nil or empty, the key is always r.RemoteAddr (XFF ignored).
//
// Edge case: if all XFF entries are trusted or unparseable, the leftmost XFF
// entry is returned (it is the closest value to the original client's claim).
//
// If RemoteAddr cannot be parsed as a valid IP, it is returned as-is so that
// callers always receive a stable, non-empty key.
func ClientIPKey(trusted []netip.Prefix) func(*http.Request) string {
	return func(r *http.Request) string {
		raw := r.RemoteAddr

		// Parse the host portion of RemoteAddr.
		host, _, err := net.SplitHostPort(raw)
		if err != nil {
			host = raw // fall back: use whole string
		}
		addr, parseErr := netip.ParseAddr(host)

		// No trusted proxies → always use RemoteAddr (never XFF).
		if len(trusted) == 0 {
			if parseErr != nil {
				return raw
			}
			return addr.String()
		}

		// If RemoteAddr can't be parsed, return it raw.
		if parseErr != nil {
			return raw
		}

		// Check whether RemoteAddr is a trusted proxy.
		if !inAny(addr, trusted) {
			return addr.String()
		}

		// RemoteAddr is trusted. Consult X-Forwarded-For.
		xff := r.Header.Get("X-Forwarded-For")
		if xff == "" {
			return addr.String()
		}

		// Walk right-to-left; skip entries that are trusted or unparseable.
		hops := strings.Split(xff, ",")
		leftmost := "" // track leftmost valid entry as fallback
		for i := len(hops) - 1; i >= 0; i-- {
			ip := strings.TrimSpace(hops[i])
			if i == 0 && leftmost == "" {
				leftmost = ip // record leftmost entry for all-trusted fallback
			}
			hopAddr, hopErr := netip.ParseAddr(ip)
			if hopErr != nil {
				continue // skip invalid entries
			}
			if leftmost == "" {
				leftmost = hopAddr.String()
			}
			if !inAny(hopAddr, trusted) {
				return hopAddr.String()
			}
		}

		// All XFF entries are trusted or invalid → return leftmost entry.
		if leftmost != "" {
			return leftmost
		}
		return addr.String()
	}
}

// inAny reports whether addr is contained in any of the given prefixes.
func inAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// RateLimitPer applies limiter l per key.
//
// Every response carries the rate-limit budget headers when the limiter
// reports them: RateLimit-Limit (bucket capacity) and RateLimit-Remaining
// (tokens left; omitted when unknown, e.g. fail-open while Redis is down).
//
// Denied requests receive an RFC7807 problem+json 429 with a real Retry-After
// header — the limiter's computed wait until the next token, rounded UP to
// whole seconds (minimum 1, since Retry-After: 0 invites an instant retry).
//
// On limiter ERROR the request is denied (fail-closed) with a 503
// problem+json: the failure is the service's, not the client's, so a 429 with
// a fabricated Retry-After would be a lie. Fail-open limiters surface errors
// as allowed results so they never trigger this path.
//
// If key(r) returns an empty string, r.RemoteAddr is used instead.
func RateLimitPer(l ratelimit.Limiter, key func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k := key(r)
			if k == "" {
				k = r.RemoteAddr
			}
			res, err := l.Allow(r.Context(), k)
			setRateLimitHeaders(w, res)
			if err != nil {
				// Fail-closed infrastructure error: not the client's fault and
				// no real wait estimate exists — 503 without Retry-After.
				httpx.WriteProblem(w, httpx.Problem{
					Status: http.StatusServiceUnavailable,
					Title:  "Service Unavailable",
					Detail: "rate limiter unavailable",
				})
				return
			}
			if !res.Allowed {
				w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds(res.RetryAfter), 10))
				httpx.WriteProblem(w, httpx.Problem{
					Status: http.StatusTooManyRequests,
					Title:  "Too Many Requests",
					Detail: "rate limit exceeded",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// setRateLimitHeaders writes the RateLimit-* budget headers from res.
// Unknown values (Limit 0, Remaining -1) are omitted rather than lied about.
func setRateLimitHeaders(w http.ResponseWriter, res ratelimit.Result) {
	if res.Limit > 0 {
		w.Header().Set("RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
	}
	if res.Remaining >= 0 {
		w.Header().Set("RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
	}
}

// retryAfterSeconds converts a wait duration to Retry-After seconds: rounded
// up, minimum 1 (zero would invite an immediate retry storm).
func retryAfterSeconds(d time.Duration) int64 {
	s := int64(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return s
}
