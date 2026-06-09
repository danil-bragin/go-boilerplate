package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/web/httpserver"

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
