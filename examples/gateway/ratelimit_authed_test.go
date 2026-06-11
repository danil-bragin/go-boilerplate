package gateway_test

// Integration tests for the authed-tier per-principal rate limiter
// (RATELIMIT_AUTHED_RPS / RATELIMIT_AUTHED_BURST): a SECOND limiter chained
// after the per-IP one, keyed by token subject (httpserver.PrincipalKey) so
// that one identity is capped across source IPs while two principals behind
// one IP get independent buckets. Anonymous requests fall back to the
// client-IP key; 0 disables the tier entirely.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listOrdersAs GETs /v1/orders with the given bearer token ("" = anonymous)
// and returns the status code plus the decoded problem code (if any).
func listOrdersAs(t *testing.T, baseURL, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/v1/orders", http.NoBody)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var prob struct {
		Code string `json:"code"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&prob)
	return resp.StatusCode, prob.Code
}

// TestGateway_AuthedRateLimit_PrincipalIsolation: with a tight authed-tier
// budget (burst 2), alice's third request is 429 RATE_LIMITED while bob —
// same client IP — still gets 200: buckets are keyed per subject, not per IP.
func TestGateway_AuthedRateLimit_PrincipalIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("RATELIMIT_AUTHED_RPS", "0.1") // negligible refill within the test window
	t.Setenv("RATELIMIT_AUTHED_BURST", "2")
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	// alice: 2 requests within burst, 3rd → 429 with the RATE_LIMITED problem.
	for i := range 2 {
		status, _ := listOrdersAs(t, baseURL, "alice")
		require.Equal(t, http.StatusOK, status, "alice request %d must be within burst", i+1)
	}
	status, code := listOrdersAs(t, baseURL, "alice")
	require.Equal(t, http.StatusTooManyRequests, status, "alice's authed bucket must be exhausted")
	assert.Equal(t, "RATE_LIMITED", code)

	// bob shares alice's client IP but not her bucket.
	status, _ = listOrdersAs(t, baseURL, "bob")
	assert.Equal(t, http.StatusOK, status, "bob must have an independent per-principal bucket")
}

// TestGateway_AuthedRateLimit_AnonymousFallsBackToIP: with auth disabled there
// is no principal, so the authed tier keys by client IP (PrincipalKey
// fallback) — repeated anonymous requests from one IP trip the tight budget.
func TestGateway_AuthedRateLimit_AnonymousFallsBackToIP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("RATELIMIT_AUTHED_RPS", "0.1")
	t.Setenv("RATELIMIT_AUTHED_BURST", "2")
	baseURL := startApp(t, broker, dsn) // GATEWAY_AUTH_DISABLED=true

	for i := range 2 {
		status, _ := listOrdersAs(t, baseURL, "")
		require.Equal(t, http.StatusOK, status, "anonymous request %d must be within burst", i+1)
	}
	status, code := listOrdersAs(t, baseURL, "")
	require.Equal(t, http.StatusTooManyRequests, status, "anonymous requests must share the IP fallback bucket")
	assert.Equal(t, "RATE_LIMITED", code)
}

// TestGateway_AuthedRateLimit_PerIPStillApplies: chain order — the per-IP
// limiter runs FIRST and both must pass: a tight per-IP budget rejects an
// authenticated caller even though the authed-tier budget is untouched.
func TestGateway_AuthedRateLimit_PerIPStillApplies(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("RATELIMIT_RPS", "0.1")
	t.Setenv("RATELIMIT_BURST", "2")
	t.Setenv("RATELIMIT_AUTHED_RPS", "1000")
	t.Setenv("RATELIMIT_AUTHED_BURST", "1000")
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	for i := range 2 {
		status, _ := listOrdersAs(t, baseURL, "alice")
		require.Equal(t, http.StatusOK, status, "request %d must be within the per-IP burst", i+1)
	}
	status, code := listOrdersAs(t, baseURL, "alice")
	require.Equal(t, http.StatusTooManyRequests, status, "per-IP limiter must still deny first (both tiers must pass)")
	assert.Equal(t, "RATE_LIMITED", code)
}

// TestGateway_AuthedRateLimit_ZeroDisables: RATELIMIT_AUTHED_RPS=0 turns the
// authed tier off entirely — a burst well past any small budget sails through
// (bounded only by the default per-IP 50/100, which it stays under).
func TestGateway_AuthedRateLimit_ZeroDisables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("RATELIMIT_AUTHED_RPS", "0")
	t.Setenv("RATELIMIT_AUTHED_BURST", "0")
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	for i := range 10 {
		status, _ := listOrdersAs(t, baseURL, "alice")
		require.Equal(t, http.StatusOK, status, "request %d must pass: authed tier is disabled", i+1)
	}
}
