package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/observability/telemetry"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newViewedProvider builds a MeterProvider with a ManualReader and the
// telemetry histogram views under test.
func newViewedProvider(t *testing.T, classic bool) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	views := telemetry.HistogramViews(classic)
	opts := make([]sdkmetric.Option, 0, len(views)+1)
	opts = append(opts, sdkmetric.WithReader(reader))
	for _, v := range views {
		opts = append(opts, sdkmetric.WithView(v))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// collect drains the manual reader and returns the metrics of the given scope.
func collect(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var out []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		out = append(out, sm.Metrics...)
	}
	return out
}

func findMetric(t *testing.T, ms []metricdata.Metrics, name string) metricdata.Metrics {
	t.Helper()
	for _, m := range ms {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("metric %q not found in %d collected metrics", name, len(ms))
	return metricdata.Metrics{}
}

// TestHistogramViews_ExponentialAppliedToDurationHistograms: the default
// (native) mode must aggregate every duration-ish histogram instrument as a
// base-2 exponential histogram on any reader of the provider.
func TestHistogramViews_ExponentialAppliedToDurationHistograms(t *testing.T) {
	mp, reader := newViewedProvider(t, false)
	m := mp.Meter("views-test")
	ctx := context.Background()

	for _, name := range []string{
		"http.server.duration",            // ms histogram (RouteTag)
		"cqrs.handler.duration_ms",        // ms histogram (cqrs.Metrics)
		"kafka.consumer.handler.duration", // future lane-B seconds histogram
		"outbox.publish_lag",              // future lane-B seconds histogram (*lag*)
	} {
		h, err := m.Float64Histogram(name)
		require.NoError(t, err)
		h.Record(ctx, 0.042)
	}

	collected := collect(t, reader)
	for _, name := range []string{
		"http.server.duration",
		"cqrs.handler.duration_ms",
		"kafka.consumer.handler.duration",
		"outbox.publish_lag",
	} {
		got := findMetric(t, collected, name)
		_, ok := got.Data.(metricdata.ExponentialHistogram[float64])
		require.True(t, ok, "%s: expected ExponentialHistogram aggregation, got %T", name, got.Data)
	}
}

// TestHistogramViews_GaugesUntouched: kafka.consumer.lag matches the *lag*
// name pattern but is a GAUGE — the view must be scoped to the histogram
// instrument kind so gauges keep their last-value aggregation.
func TestHistogramViews_GaugesUntouched(t *testing.T) {
	mp, reader := newViewedProvider(t, false)
	m := mp.Meter("views-test")

	g, err := m.Int64Gauge("kafka.consumer.lag")
	require.NoError(t, err)
	g.Record(context.Background(), 17)

	got := findMetric(t, collect(t, reader), "kafka.consumer.lag")
	_, ok := got.Data.(metricdata.Gauge[int64])
	require.True(t, ok, "kafka.consumer.lag: expected Gauge aggregation, got %T", got.Data)
}

// TestHistogramViews_ClassicFallback: TELEMETRY_CLASSIC_HISTOGRAMS=true mode
// swaps the exponential aggregation for per-signal tuned explicit buckets —
// for environments whose Prometheus cannot ingest native histograms.
func TestHistogramViews_ClassicFallback(t *testing.T) {
	mp, reader := newViewedProvider(t, true)
	m := mp.Meter("views-test")
	ctx := context.Background()

	for _, name := range []string{"http.server.duration", "pg.query.duration"} {
		h, err := m.Float64Histogram(name)
		require.NoError(t, err)
		h.Record(ctx, 3)
	}

	collected := collect(t, reader)

	httpHist := findMetric(t, collected, "http.server.duration")
	hd, ok := httpHist.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "classic mode: expected explicit-bucket Histogram, got %T", httpHist.Data)
	require.Equal(t, telemetry.ClassicBucketsMillis, hd.DataPoints[0].Bounds,
		"http.server.duration must use the ms bucket table")

	pgHist := findMetric(t, collected, "pg.query.duration")
	pd, ok := pgHist.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "classic mode: expected explicit-bucket Histogram, got %T", pgHist.Data)
	require.Equal(t, telemetry.ClassicBucketsPgSeconds, pd.DataPoints[0].Bounds,
		"pg.query.duration must use the pg seconds bucket table")
}

// TestSetupAll_WiresHistogramViews: end-to-end through SetupAll with the
// classic fallback — a duration histogram recorded via the installed global
// provider must surface in the Prometheus text exposition with OUR tuned
// bucket boundaries (le="2.5" exists in ClassicBucketsMillis but not in the
// SDK default explicit buckets), proving the views are wired into the
// provider SetupAll constructs. The exponential default rides the exact same
// wiring; its aggregation type is asserted by the manual-reader tests above.
func TestSetupAll_WiresHistogramViews(t *testing.T) {
	p, err := telemetry.SetupAll(context.Background(), telemetry.Config{
		ServiceName:       "views-e2e",
		Enabled:           false,
		MetricsPrometheus: true,
		ClassicHistograms: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	require.NotNil(t, p.MetricsHandler)

	h, err := otel.Meter("views-e2e").Float64Histogram("http.server.duration",
		metric.WithUnit("ms"))
	require.NoError(t, err)
	h.Record(context.Background(), 12)

	rec := httptest.NewRecorder()
	p.MetricsHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, `le="2.5"`,
		"classic fallback must expose the tuned ms bucket table, not SDK defaults")
}
