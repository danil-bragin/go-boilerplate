package telemetry_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-boilerplate/platform/observability/telemetry"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	noop "go.opentelemetry.io/otel/trace/noop"
)

func TestSetup_ConfiguresGlobalProvidersAndShutsDown(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "test-svc",
		Enabled:     false, // no exporter; uses noop-friendly setup
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// A tracer is obtainable and usable without panicking.
	tr := otel.Tracer("test")
	_, span := tr.Start(context.Background(), "op")
	span.End()

	require.NoError(t, shutdown(context.Background()))
}

// TestSetup_DisabledUsesNoopProvider verifies that when Enabled:false, the
// global tracer provider is the true no-op provider (zero-allocation).
func TestSetup_DisabledUsesNoopProvider(t *testing.T) {
	_, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "noop-svc",
		Enabled:     false,
	})
	require.NoError(t, err)

	require.IsType(t, noop.NewTracerProvider(), otel.GetTracerProvider())
}

// TestShutdown_FlushesEvenWithCancelledContext verifies that the shutdown
// function ignores the incoming (possibly cancelled) context and uses its own
// background-based timeout, so spans are flushed even after SIGTERM.
func TestShutdown_FlushesEvenWithCancelledContext(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "shutdown-svc",
		Enabled:     false,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Pass an already-cancelled context; shutdown must still succeed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	require.NoError(t, shutdown(ctx))
}

// TestSetup_EnabledWiresSDKProviderAndShutsDown exercises the Enabled:true
// path: a real SDK TracerProvider is installed (not the no-op), and shutdown
// completes promptly when no spans are pending (the OTLP/gRPC exporter dials
// lazily, so an empty flush does not block on the unreachable collector).
//
// Note: recording a span here would make Shutdown attempt to export it to the
// dead collector and block until the 5s shutdown deadline — that is correct
// behavior, so this test deliberately records none.
func TestSetup_EnabledWiresSDKProviderAndShutsDown(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName:  "enabled-svc",
		OTLPEndpoint: "127.0.0.1:4317", // nothing listening; lazy dial
		Enabled:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Enabled path installs a real SDK provider, not the no-op.
	require.NotEqual(t, noop.NewTracerProvider(), otel.GetTracerProvider())

	require.NoError(t, shutdown(context.Background()))
}

// TestShutdown_EnabledFlushesWithCancelledContext exercises the Enabled:true
// shutdown path with an already-cancelled context. The shutdown function must
// use its own background-based context (not the caller's) so the flush
// succeeds even when the incoming ctx is cancelled (e.g. on SIGTERM).
//
// No spans are recorded so the flush is instantaneous; this strictly tests
// that the shutdown function does not propagate the cancelled ctx to
// tp.Shutdown, which would cause it to fail immediately.
func TestShutdown_EnabledFlushesWithCancelledContext(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName:  "enabled-flush",
		OTLPEndpoint: "127.0.0.1:4317", // nothing listening; lazy dial
		Enabled:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before passing to shutdown

	// Must still return nil: no spans pending → flush is instant, and the
	// shutdown function uses context.Background() internally, not the cancelled ctx.
	require.NoError(t, shutdown(ctx))
}

// TestSetup_InstallsMeterProvider verifies that after Setup the global
// MeterProvider is NOT the noop one (a real SDK provider is installed),
// and that a counter recorded against it appears in the Prometheus text output
// when MetricsPrometheus is true (the default).
func TestSetup_InstallsMeterProvider(t *testing.T) {
	shutdown, handler, err := telemetry.SetupWithMetrics(context.Background(), telemetry.Config{
		ServiceName:       "meter-test",
		Enabled:           false, // no OTLP; only the Prometheus reader
		MetricsPrometheus: true,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	require.NotNil(t, handler, "MetricsPrometheus=true must return a non-nil http.Handler")
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// The installed provider must NOT be the noop one.
	require.NotEqual(t, noopmetric.NewMeterProvider(), otel.GetMeterProvider(),
		"expected a real SDK MeterProvider, got the noop")

	// Record a counter increment through the global API.
	counter, err := otel.Meter("test-scope").Int64Counter("test_requests_total")
	require.NoError(t, err)
	counter.Add(context.Background(), 7)

	// Scrape the Prometheus handler and assert the metric appears.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	require.Contains(t, string(body), "test_requests_total",
		"Prometheus output must contain the recorded counter")
}

// TestSetup_TraceRatioSamplerApplied verifies TELEMETRY_TRACE_RATIO wiring:
// ratio 0 → no root span is sampled; ratio 1 → every root span is sampled.
// The sampler is ParentBased(TraceIDRatioBased(ratio)), so remote parent
// decisions are still respected.
func TestSetup_TraceRatioSamplerApplied(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ratio   float64
		sampled bool
	}{
		{name: "ratio zero drops all roots", ratio: 0, sampled: false},
		{name: "ratio one samples all roots", ratio: 1, sampled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			shutdown, err := telemetry.Setup(ctx, telemetry.Config{
				ServiceName:       "ratio-test",
				OTLPEndpoint:      "localhost:1", // never reached: nothing is exported in-test
				Enabled:           true,
				MetricsPrometheus: false,
				TraceRatio:        tc.ratio,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = shutdown(cctx) // flush errors expected: no collector running
			})

			_, span := otel.Tracer("ratio-test").Start(ctx, "root")
			defer span.End()
			require.Equal(t, tc.sampled, span.SpanContext().IsSampled(),
				"TraceRatio=%v → sampled=%v", tc.ratio, tc.sampled)
		})
	}
}
