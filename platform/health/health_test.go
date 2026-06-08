package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/health"
)

func TestReadyz_AllPassReturns200(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code)
}

func TestReadyz_OneFailureReturns503(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return errors.New("down") })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
}

func TestLivez_AlwaysOK_UnlessNotLive(t *testing.T) {
	h := health.New()

	rec := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 200, rec.Code)

	h.SetNotLive() // shutdown flips liveness
	rec2 := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec2, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 503, rec2.Code)
}

// checkResponse is used to parse the JSON body from readyz responses.
type checkResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func TestReadyz_BodyListsAllCheckStatuses(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })
	h.AddReadiness("cache", func(context.Context) error { return errors.New("connection refused") })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	var resp checkResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	require.Equal(t, "unavailable", resp.Status)
	require.Equal(t, "ok", resp.Checks["db"])
	require.NotEqual(t, "ok", resp.Checks["cache"])
	require.NotEmpty(t, resp.Checks["cache"])
}

func TestReadyz_AllPassBodyAndStatus(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	var resp checkResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	require.Equal(t, "ok", resp.Status)
	require.Equal(t, "ok", resp.Checks["db"])
}

func TestReadyz_ChecksRunConcurrently(t *testing.T) {
	h := health.New()
	sleep200 := func(context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	h.AddReadiness("a", sleep200)
	h.AddReadiness("b", sleep200)
	h.AddReadiness("c", sleep200)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	start := time.Now()
	h.ReadyzHandler().ServeHTTP(rec, req)
	elapsed := time.Since(start)

	require.Equal(t, 200, rec.Code)
	// If run sequentially, total would be ~600ms. Concurrently, ~200ms.
	// Allow generous margin (450ms) to accommodate CI slowness.
	require.Less(t, elapsed, 450*time.Millisecond,
		"checks took %v; expected concurrent execution to finish in <450ms", elapsed)
}

func TestReadyz_PanicInCheckIsFailureNotCrash(t *testing.T) {
	h := health.New()
	h.AddReadiness("panic-check", func(context.Context) error {
		panic("something went very wrong")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	// This must not panic the test process.
	require.NotPanics(t, func() {
		h.ReadyzHandler().ServeHTTP(rec, req)
	})

	require.Equal(t, 503, rec.Code)

	var resp checkResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	require.Equal(t, "unavailable", resp.Status)
	require.NotEqual(t, "ok", resp.Checks["panic-check"])
	require.NotEmpty(t, resp.Checks["panic-check"])
}

func TestReadyz_NotReadyShortCircuits(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })
	h.SetNotReady()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	var resp checkResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	require.Equal(t, "unavailable", resp.Status)
}
