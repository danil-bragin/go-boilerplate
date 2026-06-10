package httpserver_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/web/httpserver"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestRouteTag_SpanNameMetricAndRoute drives a request through
// OTel → RouteTag → chi routing and asserts:
//   - the server span is renamed to "METHOD pattern" (low-cardinality);
//   - the span carries the http.route attribute;
//   - the http.server.duration histogram is recorded with
//     {method, route, status} attributes.
func TestRouteTag_SpanNameMetricAndRoute(t *testing.T) {
	// In-memory trace pipeline.
	spanExp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	// In-memory metric pipeline.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		_ = mp.Shutdown(context.Background())
	})

	r := chi.NewRouter()
	r.Use(httpserver.OTel)
	r.Use(httpserver.RouteTag())
	r.Get("/v1/orders/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders/123", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Span: name is the route PATTERN, not the raw path.
	spans := spanExp.GetSpans()
	require.NotEmpty(t, spans, "expected at least one exported span")
	span := spans[len(spans)-1]
	assert.Equal(t, "GET /v1/orders/{id}", span.Name, "span must be named METHOD pattern")
	assert.Contains(t, span.Attributes,
		attribute.String("http.route", "/v1/orders/{id}"))

	// Metric: http.server.duration{method,route,status}.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "http.server.duration must be a float64 histogram")
			for _, dp := range hist.DataPoints {
				route, _ := dp.Attributes.Value("http.route")
				method, _ := dp.Attributes.Value("http.request.method")
				status, _ := dp.Attributes.Value("http.response.status_code")
				if route.AsString() == "/v1/orders/{id}" &&
					method.AsString() == "GET" &&
					status.AsInt64() == int64(http.StatusOK) {
					found = true
					assert.Positive(t, dp.Count)
				}
			}
		}
	}
	assert.True(t, found, "http.server.duration{method,route,status} datapoint must exist")
}

// TestRouteTag_TimeoutRecords503: when http.TimeoutHandler cuts a request
// off, the client receives 503 — the RED metric must say 503 too, not the
// status the abandoned handler wrote on its dead writer.
func TestRouteTag_TimeoutRecords503(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		_ = mp.Shutdown(context.Background())
	})

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	r := chi.NewRouter()
	r.Use(httpserver.Timeout(30 * time.Millisecond))
	r.Use(httpserver.RouteTag())
	done := make(chan struct{})
	r.Get("/v1/slow", func(w http.ResponseWriter, req *http.Request) {
		defer close(done)
		select {
		case <-req.Context().Done(): // TimeoutHandler cancelled us
		case <-release:
		}
		w.WriteHeader(http.StatusOK) // lands on the abandoned writer
	})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/slow", http.NoBody))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, "client must see the TimeoutHandler 503")

	// RouteTag records on the inner (abandoned) goroutine — wait for it.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler goroutine did not finish")
	}

	// The datapoint is recorded just after the handler returns on the
	// abandoned goroutine; poll until it shows up.
	var status int64 = -1
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "http.server.duration" {
					continue
				}
				hist, ok := m.Data.(metricdata.Histogram[float64])
				if !ok {
					continue
				}
				for _, dp := range hist.DataPoints {
					route, _ := dp.Attributes.Value("http.route")
					if route.AsString() == "/v1/slow" {
						s, _ := dp.Attributes.Value("http.response.status_code")
						status = s.AsInt64()
						return true
					}
				}
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "a datapoint for the timed-out route must exist")

	assert.Equal(t, int64(http.StatusServiceUnavailable), status,
		"timed-out request must be recorded as 503, not the handler's intended status")
}

// TestAccessLog_UsesRoutePattern: the access log line must carry the route
// PATTERN (low-cardinality field for aggregation) alongside the raw path.
func TestAccessLog_UsesRoutePattern(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(log.Into(req.Context(), logger)))
		})
	})
	r.Use(httpserver.AccessLog)
	r.Use(httpserver.RouteTag()) // supplies the route pattern to AccessLog
	r.Get("/v1/orders/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders/456", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)

	line := buf.String()
	assert.Contains(t, line, `"route":"/v1/orders/{id}"`, "access log must carry the route pattern")
	assert.Contains(t, line, `"path":"/v1/orders/456"`, "raw path stays for debugging")
}
