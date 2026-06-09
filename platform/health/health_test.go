package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
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

// TestHealth_MountRegistersEndpoints verifies that Mount registers /livez and
// /readyz on a chi router. GET /livez → 200, GET /readyz → 200 by default;
// adding a failing readiness check makes /readyz → 503 JSON.
func TestHealth_MountRegistersEndpoints(t *testing.T) {
	t.Run("livez returns 200", func(t *testing.T) {
		mux := chi.NewRouter()
		h := health.New()
		health.Mount(mux, h)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("readyz returns 200 with no checks", func(t *testing.T) {
		mux := chi.NewRouter()
		h := health.New()
		health.Mount(mux, h)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("readyz returns 503 with failing check", func(t *testing.T) {
		mux := chi.NewRouter()
		h := health.New()
		h.AddReadiness("db", func(context.Context) error {
			return errors.New("connection refused")
		})
		health.Mount(mux, h)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Contains(t, rec.Header().Get("Content-Type"), "application/json")

		var resp checkResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Equal(t, "unavailable", resp.Status)
	})
}

// TestHealth_CheckFunc verifies that health.CheckFunc adapts a plain
// func(ctx) error into a Check, enabling callers to register pg/kafka checks
// without creating an import cycle.
func TestHealth_CheckFunc(t *testing.T) {
	t.Run("passing check func", func(t *testing.T) {
		var called bool
		check := health.CheckFunc(func(_ context.Context) error {
			called = true
			return nil
		})

		err := check(context.Background())
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("failing check func", func(t *testing.T) {
		check := health.CheckFunc(func(_ context.Context) error {
			return errors.New("db down")
		})

		err := check(context.Background())
		require.EqualError(t, err, "db down")
	})
}

// TestReadyz_HandlerReturnsAtDeadlineEvenIfCheckIgnoresCtx verifies that the
// HTTP handler returns at the per-check timeout even when the check function
// ignores its context and sleeps longer. The handler must NOT block on
// wg.Wait() for the full sleep duration; it must unblock at the deadline.
func TestReadyz_HandlerReturnsAtDeadlineEvenIfCheckIgnoresCtx(t *testing.T) {
	h := health.New()
	// This check sleeps 800ms regardless of context cancellation.
	h.AddReadiness("slow", func(_ context.Context) error {
		time.Sleep(800 * time.Millisecond)
		return nil
	})
	h.SetCheckTimeout(50 * time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	start := time.Now()
	h.ReadyzHandler().ServeHTTP(rec, req)
	elapsed := time.Since(start)

	// Handler must return well before the 800ms sleep finishes.
	require.Less(t, elapsed, 300*time.Millisecond,
		"handler took %v; expected to return at the 50ms deadline, not wait for the 800ms sleep", elapsed)

	// The timed-out check must be recorded as a failure, causing 503.
	require.Equal(t, 503, rec.Code)

	var resp checkResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "unavailable", resp.Status)
	require.NotEmpty(t, resp.Checks["slow"], "timed-out check must appear in response")
	require.NotEqual(t, "ok", resp.Checks["slow"])
}
