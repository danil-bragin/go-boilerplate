package httpserver_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"go-boilerplate/platform/security/auth"
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
		req.Header.Set("Access-Control-Request-Method", "POST")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code, "disallowed preflight must be rejected")
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"), "disallowed origin must not get CORS header")
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Methods"))
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Headers"))
		require.Equal(t, "Origin", rec.Header().Get("Vary"))
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
		require.Equal(t, "Origin", rec.Header().Get("Vary"),
			"Vary: Origin must be emitted even without an Origin header — caches must always key on Origin")
	})

	t.Run("OPTIONS without Access-Control-Request-Method is not a preflight", func(t *testing.T) {
		// A plain OPTIONS request (no Access-Control-Request-Method) is an
		// ordinary cross-origin request, not a preflight: it must reach the
		// handler instead of being short-circuited with 204.
		req := httptest.NewRequest(http.MethodOptions, "/orders", http.NoBody)
		req.Header.Set("Origin", "https://example.com")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, "plain OPTIONS must reach the handler")
		require.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Methods"),
			"preflight-only headers must not appear on a non-preflight response")
	})

	t.Run("disallowed preflight gets problem+json body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/orders", http.NoBody)
		req.Header.Set("Origin", "https://evil.com")
		req.Header.Set("Access-Control-Request-Method", "POST")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code)
		require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
		require.Contains(t, rec.Body.String(), "Forbidden")
	})

	t.Run("Vary Origin always present when Origin sent", func(t *testing.T) {
		for _, origin := range []string{"https://example.com", "https://evil.com"} {
			req := httptest.NewRequest(http.MethodGet, "/orders", http.NoBody)
			req.Header.Set("Origin", origin)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, "Origin", rec.Header().Get("Vary"),
				"Vary: Origin must be set for origin %s (cache poisoning defence)", origin)
		}
	})
}

// TestCORS_EmptyOriginsDenyAll: the zero-config default is deny-all — no
// Access-Control-Allow-Origin is ever emitted, for any origin, on either the
// preflight or the actual request path.
func TestCORS_EmptyOriginsDenyAll(t *testing.T) {
	h := httpserver.CORS(httpserver.CORSOptions{})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	t.Run("actual request gets no ACAO", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Origin", "https://anything.example")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, "request itself still served (CORS is a browser control)")
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
		require.Equal(t, "Origin", rec.Header().Get("Vary"))
	})

	t.Run("preflight denied", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/", http.NoBody)
		req.Header.Set("Origin", "https://anything.example")
		req.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code)
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Methods"))
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

// ---------------------------------------------------------------------------
// PrincipalKey
// ---------------------------------------------------------------------------

// TestPrincipalKey_AuthenticatedUsesSubject: with a Principal in the request
// context the key is "sub:"+Subject — the fallback is never consulted.
func TestPrincipalKey_AuthenticatedUsesSubject(t *testing.T) {
	fallbackCalled := false
	fallback := func(*http.Request) string {
		fallbackCalled = true
		return "ip:1.2.3.4"
	}
	key := httpserver.PrincipalKey(fallback)

	req := newReq("1.2.3.4:50001", "")
	req = req.WithContext(auth.Into(req.Context(), auth.Principal{Subject: "user-42"}))

	require.Equal(t, "sub:user-42", key(req))
	require.False(t, fallbackCalled, "fallback must not be consulted for an authenticated request")
}

// TestPrincipalKey_TwoSubjectsGetDistinctKeys: principal isolation — two
// subjects sharing one client IP land in independent buckets.
func TestPrincipalKey_TwoSubjectsGetDistinctKeys(t *testing.T) {
	key := httpserver.PrincipalKey(httpserver.ClientIPKey(nil))

	reqA := newReq("1.2.3.4:50001", "")
	reqA = reqA.WithContext(auth.Into(reqA.Context(), auth.Principal{Subject: "alice"}))
	reqB := newReq("1.2.3.4:50002", "")
	reqB = reqB.WithContext(auth.Into(reqB.Context(), auth.Principal{Subject: "bob"}))

	require.Equal(t, "sub:alice", key(reqA))
	require.Equal(t, "sub:bob", key(reqB))
	require.NotEqual(t, key(reqA), key(reqB), "same IP, different subjects → different buckets")
}

// TestPrincipalKey_AnonymousFallsBack: no Principal in context → the fallback
// key function (here ClientIPKey) decides.
func TestPrincipalKey_AnonymousFallsBack(t *testing.T) {
	key := httpserver.PrincipalKey(httpserver.ClientIPKey(nil))
	require.Equal(t, "1.2.3.4", key(newReq("1.2.3.4:50001", "")))
}

// TestPrincipalKey_EmptySubjectFallsBack: a Principal with an empty Subject
// must not collapse every such caller into one shared "sub:" bucket.
func TestPrincipalKey_EmptySubjectFallsBack(t *testing.T) {
	key := httpserver.PrincipalKey(httpserver.ClientIPKey(nil))
	req := newReq("9.8.7.6:50001", "")
	req = req.WithContext(auth.Into(req.Context(), auth.Principal{Subject: ""}))
	require.Equal(t, "9.8.7.6", key(req))
}

// TestRateLimitPer_PrincipalBucketIsolation: end-to-end through RateLimitPer —
// exhausting alice's bucket leaves bob's untouched even from the same IP, and
// an anonymous caller from that IP has its own (fallback) bucket.
func TestRateLimitPer_PrincipalBucketIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	lim := ratelimit.NewMemory(10, 1) // burst=1: one token per key
	t.Cleanup(lim.Close)

	h := httpserver.RateLimitPer(lim, httpserver.PrincipalKey(httpserver.ClientIPKey(nil)))(okHandler)

	as := func(subject string) *http.Request {
		req := newReq("1.2.3.4:50001", "")
		if subject != "" {
			req = req.WithContext(auth.Into(req.Context(), auth.Principal{Subject: subject}))
		}
		return req
	}

	// alice: token 1 OK, token 2 → 429.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, as("alice"))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, as("alice"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code, "alice's bucket is exhausted")

	// bob (same IP): independent bucket — still OK.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, as("bob"))
	require.Equal(t, http.StatusOK, rec.Code, "bob must not share alice's bucket")

	// anonymous (same IP): falls back to the IP bucket — also independent.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, as(""))
	require.Equal(t, http.StatusOK, rec.Code, "anonymous IP bucket is independent of principal buckets")
}

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

// stubLimiter is a Limiter returning a fixed Result, for deterministic
// header-emission tests.
type stubLimiter struct{ res ratelimit.Result }

func (l stubLimiter) Allow(context.Context, string) (ratelimit.Result, error) {
	return l.res, nil
}

// TestRateLimitPer_ResetHeader: a known Reset delta is emitted as
// RateLimit-Reset in ceiled whole seconds.
func TestRateLimitPer_ResetHeader(t *testing.T) {
	h := httpserver.RateLimitPer(stubLimiter{res: ratelimit.Result{
		Allowed: true, Limit: 5, Remaining: 3, Reset: 1500 * time.Millisecond,
	}}, httpserver.ClientIPKey(nil))(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReq("1.2.3.4:50001", ""))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "2", rec.Header().Get("RateLimit-Reset"),
		"Reset must be emitted as delta seconds, rounded up")
}

// TestRateLimitPer_UnknownBudgetHeadersOmitted: the unified unknown sentinel
// (-1 for Limit and Remaining, 0 for Reset) suppresses the corresponding
// headers — better absent than lied about.
func TestRateLimitPer_UnknownBudgetHeadersOmitted(t *testing.T) {
	h := httpserver.RateLimitPer(stubLimiter{res: ratelimit.Result{
		Allowed: true, Limit: -1, Remaining: -1,
	}}, httpserver.ClientIPKey(nil))(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReq("1.2.3.4:50001", ""))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Header().Get("RateLimit-Limit"))
	require.Empty(t, rec.Header().Get("RateLimit-Remaining"))
	require.Empty(t, rec.Header().Get("RateLimit-Reset"))
}

// errLimiter is a Limiter stub whose Allow always fails (fail-closed limiter
// surfacing an infrastructure error, e.g. Redis down with fail-open disabled).
type errLimiter struct{ err error }

func (l errLimiter) Allow(context.Context, string) (ratelimit.Result, error) {
	return ratelimit.Result{}, l.err
}

// TestRateLimitPer_LimiterError503 verifies the fail-closed error path: a
// limiter infrastructure error is NOT the client's fault — the response must
// be 503 problem+json (service trouble), not a 429 with a made-up
// Retry-After: 1 that invites an immediate retry storm against a broken
// backend.
func TestRateLimitPer_LimiterError503(t *testing.T) {
	h := httpserver.RateLimitPer(errLimiter{err: errors.New("redis: connection refused")},
		httpserver.ClientIPKey(nil))(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReq("1.2.3.4:50001", ""))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"limiter error under fail-closed must be 503, not 429")
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	require.Empty(t, rec.Header().Get("Retry-After"),
		"no real wait estimate exists on a limiter error — do not fabricate Retry-After")
	require.NotContains(t, rec.Body.String(), "redis",
		"limiter internals must not leak to the client")
}

// TestRateLimitPer_429ProblemAndHeaders verifies the 429 ergonomics: a real
// Retry-After (ceiled seconds), RateLimit-Limit / RateLimit-Remaining headers,
// and an RFC7807 problem+json body.
func TestRateLimitPer_429ProblemAndHeaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: memory limiter uses real time — not suitable for -short")
	}
	lim := ratelimit.NewMemory(0.5, 2, ratelimit.WithClock(time.Now)) // 1 token / 2s, burst 2
	t.Cleanup(lim.Close)

	keyFn := httpserver.ClientIPKey(nil)
	h := httpserver.RateLimitPer(lim, keyFn)(okHandler)

	// Allowed requests carry the rate-limit budget headers.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, newReq("1.2.3.4:1", ""))
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, "2", rec1.Header().Get("RateLimit-Limit"))
	require.Equal(t, "1", rec1.Header().Get("RateLimit-Remaining"))

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, newReq("1.2.3.4:1", ""))
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, "0", rec2.Header().Get("RateLimit-Remaining"))

	// Denied request: problem+json + headers with the real wait (~2s → "2").
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, newReq("1.2.3.4:1", ""))
	require.Equal(t, http.StatusTooManyRequests, rec3.Code)
	require.Equal(t, "application/problem+json", rec3.Header().Get("Content-Type"))
	require.Contains(t, rec3.Body.String(), "rate limit exceeded")
	require.Equal(t, "2", rec3.Header().Get("RateLimit-Limit"))
	require.Equal(t, "0", rec3.Header().Get("RateLimit-Remaining"))
	require.Equal(t, "2", rec3.Header().Get("Retry-After"),
		"Retry-After must reflect the real refill wait (ceil seconds), not a hardcoded 1")
}
