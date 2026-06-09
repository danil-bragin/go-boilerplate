package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"go-boilerplate/platform/web/httpserver"
	"go-boilerplate/platform/web/ratelimit"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SecurityHeaders
// ---------------------------------------------------------------------------

func TestSecurityHeaders_SetsHeaders(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := httpserver.SecurityHeaders(inner)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"), "X-Content-Type-Options must be nosniff")
	require.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"), "X-Frame-Options must be DENY")
	require.Equal(t, "no-referrer", rec.Header().Get("Referrer-Policy"), "Referrer-Policy must be no-referrer")
	require.Equal(t, "default-src 'none'", rec.Header().Get("Content-Security-Policy"), "CSP must be restrictive for API")
	require.Equal(t, "0", rec.Header().Get("X-XSS-Protection"), "X-XSS-Protection must be 0 (disabled)")
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

func TestCORS_PreflightAndAllowOrigin(t *testing.T) {
	opts := httpserver.CORSOptions{
		AllowedOrigins: []string{"https://example.com"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Content-Type", "Authorization"},
		MaxAge:         600,
	}
	corsMiddleware := httpserver.CORS(opts)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := corsMiddleware(inner)

	t.Run("preflight from allowed origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/orders", http.NoBody)
		req.Header.Set("Origin", "https://example.com")
		req.Header.Set("Access-Control-Request-Method", "POST")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code, "preflight must return 204")
		require.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
		require.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Methods"))
		require.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Headers"))
		require.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
	})

	t.Run("preflight from disallowed origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/orders", http.NoBody)
		req.Header.Set("Origin", "https://evil.com")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code, "preflight still returns 204 but no CORS headers")
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"), "disallowed origin must not get CORS header")
	})

	t.Run("actual request from allowed origin gets ACAO header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/orders", http.NoBody)
		req.Header.Set("Origin", "https://example.com")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("non-CORS request passes through unchanged", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/orders", http.NoBody)
		// No Origin header → not a CORS request.

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})
}

// ---------------------------------------------------------------------------
// RateLimit
// ---------------------------------------------------------------------------

// TestRateLimit_429WhenExceeded creates a limiter of rps=1, burst=1.
// The first request should pass (burst=1 allows one token immediately).
// The second immediate request must receive 429 because the bucket is empty.
func TestRateLimit_429WhenExceeded(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// rps=1, burst=1: exactly one token in the bucket at start.
	h := httpserver.RateLimit(1, 1)(inner)

	// First request: should consume the single token and succeed.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	require.Equal(t, http.StatusOK, rec1.Code, "first request within burst must succeed")

	// Second immediate request: bucket empty, must be rate-limited.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code, "second immediate request must be rate-limited (429)")
}

// ---------------------------------------------------------------------------
// RateLimitPer + ClientIPKey
// ---------------------------------------------------------------------------

// newReq builds a test request with the given remote address and optional
// X-Forwarded-For header.
func newReq(remoteAddr, xff string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = remoteAddr
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	return req
}

// okHandler always returns 200 OK.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// TestRateLimitPer_PerIPIsolation verifies that two distinct RemoteAddrs have
// independent buckets: exhausting A's bucket does not affect B.
func TestRateLimitPer_PerIPIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	// burst=2: each IP gets exactly 2 tokens.
	lim := ratelimit.NewMemory(10, 2)
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(nil)
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// Exhaust A's bucket (2 requests → OK, 3rd → 429).
	for i := range 2 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, newReq("1.2.3.4:50001", ""))
		require.Equal(t, http.StatusOK, rec.Code, "request %d for A must succeed (within burst)", i+1)
	}
	recA3 := httptest.NewRecorder()
	h.ServeHTTP(recA3, newReq("1.2.3.4:50001", ""))
	require.Equal(t, http.StatusTooManyRequests, recA3.Code, "3rd request for A must be rate-limited")
	require.Equal(t, "1", recA3.Header().Get("Retry-After"), "429 must carry Retry-After: 1")

	// B's bucket is independent — first request still succeeds.
	recB := httptest.NewRecorder()
	h.ServeHTTP(recB, newReq("5.6.7.8:50002", ""))
	require.Equal(t, http.StatusOK, recB.Code, "B's first request must succeed regardless of A being rate-limited")
}

// TestRateLimitPer_XFFIgnoredFromUntrustedPeer verifies that when no trusted
// proxies are configured, the X-Forwarded-For header is ignored and the key is
// always RemoteAddr. Specifically, spoofing a different XFF IP does not create
// a separate bucket — the same RemoteAddr bucket is used.
func TestRateLimitPer_XFFIgnoredFromUntrustedPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	// nil trusted proxies → XFF is never honoured.
	lim := ratelimit.NewMemory(10, 1)
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(nil) // no trusted proxies
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// First request from 1.2.3.4 with spoofed XFF: consumes the single token.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, newReq("1.2.3.4:50001", "9.9.9.9"))
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second request with a DIFFERENT spoofed XFF but SAME RemoteAddr: 429 because
	// the bucket key is RemoteAddr, not XFF.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, newReq("1.2.3.4:50001", "8.8.8.8"))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code, "XFF spoofing must not bypass per-RemoteAddr rate limit")
}

// TestRateLimitPer_XFFHonouredBehindTrustedProxy verifies that when the
// RemoteAddr belongs to a trusted proxy prefix, the X-Forwarded-For header is
// consulted and different client IPs receive separate buckets.
func TestRateLimitPer_XFFHonouredBehindTrustedProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	// Trust the proxy subnet 10.0.0.0/8 so that RemoteAddr 10.0.0.1 is trusted.
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	lim := ratelimit.NewMemory(10, 1) // burst=1 per key
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(trusted)
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// Two requests arrive via the same trusted proxy (RemoteAddr=10.0.0.1)
	// but from different real client IPs in XFF → separate buckets.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, newReq("10.0.0.1:9000", "192.168.1.1")) // XFF client A
	require.Equal(t, http.StatusOK, rec1.Code, "client A first request must succeed")

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, newReq("10.0.0.1:9000", "192.168.1.2")) // XFF client B
	require.Equal(t, http.StatusOK, rec2.Code, "client B first request must succeed (separate bucket)")

	// A's bucket is now exhausted; a second request from A gets 429.
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, newReq("10.0.0.1:9000", "192.168.1.1"))
	require.Equal(t, http.StatusTooManyRequests, rec3.Code, "client A second request must be rate-limited")
}

// TestRateLimitPer_XFFRightToLeftSkipsTrustedHops verifies the right-to-left
// walk: given XFF "client, proxy2" where proxy2 is trusted, the extracted key
// is the client IP, not proxy2.
func TestRateLimitPer_XFFRightToLeftSkipsTrustedHops(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	// Trust 10.0.0.0/8 (proxy2 = 10.0.0.2 is trusted).
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	lim := ratelimit.NewMemory(10, 1)
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(trusted)
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// RemoteAddr is the ingress proxy (also trusted); XFF = "clientIP, proxy2".
	// Right-to-left: proxy2 (10.0.0.2) is trusted → skip; clientIP (2.3.4.5) is
	// untrusted → use it as the key.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, newReq("10.0.0.1:9000", "2.3.4.5, 10.0.0.2"))
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second request from the same client IP must hit the same bucket → 429.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, newReq("10.0.0.1:9000", "2.3.4.5, 10.0.0.2"))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code, "same client IP must reuse the same bucket")
}

// TestRateLimitPer_429BodyMatchesRateLimit verifies that the 429 response body
// produced by RateLimitPer matches the plain-text body used by the legacy
// RateLimit middleware ("rate limit exceeded\n").
func TestRateLimitPer_429BodyMatchesRateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	lim := ratelimit.NewMemory(10, 1)
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(nil)
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// Exhaust the bucket.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, newReq("1.2.3.4:1", ""))
	require.Equal(t, http.StatusOK, rec1.Code)

	// Get the 429.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, newReq("1.2.3.4:1", ""))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
	require.Equal(t, "rate limit exceeded\n", rec2.Body.String(), "429 body must match RateLimit shape")
}
