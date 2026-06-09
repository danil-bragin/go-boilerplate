package httpserver_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/web/httpserver"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// ---------------------------------------------------------------------------
// RequestID
// ---------------------------------------------------------------------------

func TestRequestID_AddsHeaderAndContext(t *testing.T) {
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	require.NotEmpty(t, seen)
	require.Equal(t, seen, rec.Header().Get("X-Request-Id"))
}

func TestRequestID_ReusesIncomingHeader(t *testing.T) {
	const incoming = "abc-123-existing"
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Request-Id", incoming)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, incoming, seen)
	require.Equal(t, incoming, rec.Header().Get("X-Request-Id"))
}

// A4 – sanitize incoming X-Request-Id
func TestRequestID_RejectsCRLF(t *testing.T) {
	const malicious = "abc\r\ninjected"
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Request-Id", malicious)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.NotEqual(t, malicious, seen, "CRLF id must not be reused")
	require.NotContains(t, seen, "\r")
	require.NotContains(t, seen, "\n")
	require.Len(t, seen, 32, "replacement must be a fresh 32-hex id")
}

func TestRequestID_RejectsOversize(t *testing.T) {
	oversize := strings.Repeat("a", 200)
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Request-Id", oversize)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.NotEqual(t, oversize, seen, "oversize id must not be reused")
	require.Len(t, seen, 32, "replacement must be a fresh 32-hex id")
}

// ---------------------------------------------------------------------------
// Recover
// ---------------------------------------------------------------------------

func TestRecover_TurnsPanicInto500Problem(t *testing.T) {
	h := httpserver.Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	require.Equal(t, 500, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}

// A1 – re-panic http.ErrAbortHandler
func TestRecover_RepanicsErrAbortHandler(t *testing.T) {
	h := httpserver.Recover(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)

	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		h.ServeHTTP(rec, req)
	})
}

// A2 – do not write after headers are committed
func TestRecover_DoesNotWriteAfterCommit(t *testing.T) {
	h := httpserver.Recover(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		panic("too late")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	require.Equal(t, 200, rec.Code)
	require.Equal(t, "partial", rec.Body.String())
}

// ---------------------------------------------------------------------------
// AccessLog
// ---------------------------------------------------------------------------

// B1 – access log middleware
func TestAccessLog_LogsRequest(t *testing.T) {
	var buf bytes.Buffer
	logger, sync := log.New(log.Config{Level: "info", Format: "json"}, &buf)
	defer sync() //nolint:errcheck

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Chain: RequestID → AccessLog → inner so request-id is in context
	h := httpserver.RequestID(httpserver.AccessLog(inner))

	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	// Inject logger into request context
	req = req.WithContext(log.Into(req.Context(), logger))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	line := buf.String()
	require.Contains(t, line, `"msg":"http request"`)
	require.Contains(t, line, `"method":"GET"`)
	require.Contains(t, line, `"status":200`)
}

// ---------------------------------------------------------------------------
// MaxBytes
// ---------------------------------------------------------------------------

// B2 – max bytes middleware
func TestMaxBytes_LimitsBody(t *testing.T) {
	const limit = 16

	var readErr error
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	h := httpserver.MaxBytes(limit)(inner)

	body := strings.Repeat("x", 100)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Error(t, readErr, "reading beyond limit must error")
}

// ---------------------------------------------------------------------------
// Timeout
// ---------------------------------------------------------------------------

// B3 – timeout middleware
func TestTimeout_Returns503OnSlowHandler(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	h := httpserver.Timeout(20 * time.Millisecond)(inner)

	req := httptest.NewRequest(http.MethodGet, "/slow", http.NoBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

// ---------------------------------------------------------------------------
// OTel middleware
// ---------------------------------------------------------------------------

// TestOTelMiddleware_CreatesServerSpan verifies that the OTel middleware starts
// a server span per request. It uses an in-memory exporter so the test
// exercises the full SDK pipeline without a real collector.
func TestOTelMiddleware_CreatesServerSpan(t *testing.T) {
	// Install an in-memory SDK tracer provider as the global provider so
	// otelhttp picks it up.
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// Swap in the SDK provider and restore the previous one after the test.
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := httpserver.OTel(inner)

	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	spans := exp.GetSpans()
	require.NotEmpty(t, spans, "OTel middleware must export at least one span per request")
}
