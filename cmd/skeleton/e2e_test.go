package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTestApp creates, starts, and registers cleanup for an app.
// Callers that need extra env vars (e.g. HTTP_MAX_BODY_BYTES) must set them
// BEFORE calling this helper.
func startTestApp(t *testing.T) (*app, string) {
	t.Helper()
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0") // harness admin server: ephemeral port
	t.Setenv("DRAIN_GRACE", "0")               // no LB drain window needed in tests
	t.Setenv("OTEL_ENABLED", "false")
	a, err := newApp(context.Background())
	require.NoError(t, err)
	require.NoError(t, a.start())
	t.Cleanup(func() { _ = a.stop(context.Background()) })
	return a, "http://" + a.server.Addr()
}

func newClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Second}
}

// TestE2E_PingReturnsJSONWithRequestID verifies the /ping handler returns 200
// JSON and the RequestID middleware injects a non-empty X-Request-Id header.
func TestE2E_PingReturnsJSONWithRequestID(t *testing.T) {
	_, base := startTestApp(t)
	client := newClient()

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ping", http.NoBody)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "pong")

	rid := resp.Header.Get("X-Request-Id")
	assert.NotEmpty(t, rid, "X-Request-Id header must be set by RequestID middleware")
}

// TestE2E_RequestIDPropagatedFromClient verifies that a valid client-supplied
// X-Request-Id is echoed back unchanged.
func TestE2E_RequestIDPropagatedFromClient(t *testing.T) {
	_, base := startTestApp(t)
	client := newClient()

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ping", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("X-Request-Id", "e2e-test-id-123")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "e2e-test-id-123", resp.Header.Get("X-Request-Id"),
		"valid client-supplied request ID must be echoed back")
}

// TestE2E_RequestIDSanitizedOnTheWire verifies that an oversize X-Request-Id
// value is replaced with a fresh 32-hex id by the RequestID middleware.
func TestE2E_RequestIDSanitizedOnTheWire(t *testing.T) {
	_, base := startTestApp(t)
	client := newClient()

	// Use an oversize value (>128 chars) to trigger sanitisation without
	// risking client-side CRLF rejection.
	oversized := strings.Repeat("A", 200)

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ping", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("X-Request-Id", oversized)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	rid := resp.Header.Get("X-Request-Id")
	assert.NotEqual(t, oversized, rid, "oversize id must be replaced")
	assert.Len(t, rid, 32, "replacement id must be a 32-hex string")
}

// TestE2E_PanicReturns500ProblemJSON verifies that a panicking handler is
// recovered and returns a 500 application/problem+json response.
func TestE2E_PanicReturns500ProblemJSON(t *testing.T) {
	_, base := startTestApp(t)
	client := newClient()

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/boom", http.NoBody)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var problem map[string]any
	require.NoError(t, json.Unmarshal(body, &problem))
	assert.EqualValues(t, 500, problem["status"], "problem JSON must carry status:500")
}

// TestE2E_ReadyzJSONAndLivez verifies /readyz returns the expected JSON
// structure and /livez returns 200 "ok".
func TestE2E_ReadyzJSONAndLivez(t *testing.T) {
	_, base := startTestApp(t)
	client := newClient()
	ctx := context.Background()

	// --- /readyz ---
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", http.NoBody)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var readyz struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	require.NoError(t, json.Unmarshal(body, &readyz))
	assert.Equal(t, "ok", readyz.Status)
	assert.Equal(t, "ok", readyz.Checks["self"])

	// --- /livez ---
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/livez", http.NoBody)
	require.NoError(t, err)

	resp2, err := client.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	assert.Equal(t, "ok", string(body2))
}

// TestE2E_ReadyzFlipsTo503OnShutdownStart verifies that after SetNotReady the
// /readyz endpoint returns 503 with status:"unavailable".
func TestE2E_ReadyzFlipsTo503OnShutdownStart(t *testing.T) {
	a, base := startTestApp(t)
	client := newClient()
	ctx := context.Background()

	// Confirm still ready.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", http.NoBody)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Simulate shutdown start.
	a.health.SetNotReady()

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", http.NoBody)
	require.NoError(t, err)
	resp2, err := client.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)

	body, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)

	var readyz struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(body, &readyz))
	assert.Equal(t, "unavailable", readyz.Status)
}

// TestE2E_GracefulDrainCompletesInflightRequest asserts that a request already
// in-flight (the /slow handler sleeping 150ms) completes successfully even
// after graceful shutdown is initiated, and that the server refuses new
// connections after shutdown completes.
func TestE2E_GracefulDrainCompletesInflightRequest(t *testing.T) {
	a, base := startTestApp(t)

	inflightDone := make(chan int, 1) // receives status code

	// Fire the slow request before initiating shutdown.
	go func() {
		// Use a generous timeout so the in-flight request outlasts shutdown.
		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/slow", http.NoBody)
		if err != nil {
			inflightDone <- 0
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			inflightDone <- 0
			return
		}
		resp.Body.Close()
		inflightDone <- resp.StatusCode
	}()

	// Give the in-flight request a moment to reach the handler.
	time.Sleep(30 * time.Millisecond)

	// Graceful shutdown: allow up to 5s for existing requests to drain.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, a.stop(stopCtx))

	// (a) The in-flight /slow request must complete successfully.
	select {
	case status := <-inflightDone:
		assert.Equal(t, http.StatusOK, status, "in-flight request must complete with 200")
	case <-time.After(4 * time.Second):
		t.Fatal("in-flight request did not complete within 4s after stop")
	}

	// (b) After stop, new connections must fail.
	newClient2 := &http.Client{Timeout: 500 * time.Millisecond}
	ctx2 := context.Background()
	req, err := http.NewRequestWithContext(ctx2, http.MethodGet, base+"/ping", http.NoBody)
	require.NoError(t, err)
	resp2, err := newClient2.Do(req)
	if resp2 != nil {
		resp2.Body.Close()
	}
	assert.Error(t, err, "connections after shutdown must fail")
	var netErr net.Error
	if assert.ErrorAs(t, err, &netErr) {
		_ = netErr // connection refused or timeout — both are acceptable
	}
}

// TestE2E_MaxBodyRejectsLargeBody verifies that the MaxBytes middleware causes
// the handler to return 400 when the body exceeds the configured limit.
func TestE2E_MaxBodyRejectsLargeBody(t *testing.T) {
	// Override the limit to something tiny before building the app.
	t.Setenv("HTTP_MAX_BODY_BYTES", "64")

	_, base := startTestApp(t)
	client := newClient()
	ctx := context.Background()

	// Send a body that is larger than the 64-byte limit.
	bigBody := bytes.Repeat([]byte("x"), 200)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/echo", bytes.NewReader(bigBody))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"body exceeding MaxBodyBytes must yield 400 from the echo handler")
}
