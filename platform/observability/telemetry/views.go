package telemetry

import (
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Latency-tail histogram engine (round 5).
//
// Every duration-ish histogram instrument is aggregated as a base-2
// EXPONENTIAL histogram by default. Exponential histograms keep ~1.1% relative
// error across the full range (microseconds → minutes) with a fixed memory
// budget, and the collector/Prometheus pipeline stores them as Prometheus
// NATIVE histograms — accurate p99s without hand-tuning bucket tables per
// signal. MaxSize 160 / MaxScale 20 match the OTel SDK defaults: 160 buckets
// cover >5 orders of magnitude at the auto-downscaled resolution.
//
// The views are name-pattern scoped (*duration*, *lag* — the former also
// covers *.duration_ms names) AND kind-scoped to histogram instruments only:
// kafka.consumer.lag is a GAUGE that matches *lag* by name, and a view must
// never rewrite a gauge's last-value aggregation.
//
// Fallback (TELEMETRY_CLASSIC_HISTOGRAMS=true): environments whose Prometheus
// cannot ingest native histograms (managed offerings, remote-write hops that
// strip them, Prometheus <2.40) flip to per-signal tuned EXPLICIT buckets —
// the classic *_bucket representation every backend understands. The bucket
// tables below are tuned per signal so the interesting tail region has
// resolution (the SDK default explicit buckets stop resolving past 10s and
// are identical for every instrument).

// Classic-fallback explicit bucket tables (TELEMETRY_CLASSIC_HISTOGRAMS=true).
// Exported so tests (and curious operators) can reference the exact tables.
var (
	// ClassicBucketsMillis: http.server.duration + cqrs.handler.duration_ms
	// (both record MILLISECONDS): 1ms .. 10s.
	ClassicBucketsMillis = []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	// ClassicBucketsKafkaSeconds: kafka consumer-handler + producer-publish
	// durations (seconds): 1ms .. 30s (handlers may legitimately run long).
	ClassicBucketsKafkaSeconds = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	// ClassicBucketsOutboxSeconds: outbox publish lag (seconds): 5ms .. 5m —
	// a drained outbox publishes within one poll tick, a backlog grows to minutes.
	ClassicBucketsOutboxSeconds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 30, 60, 120, 300}
	// ClassicBucketsPgSeconds: pg.query.duration (seconds): 0.1ms .. 5s.
	ClassicBucketsPgSeconds = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// ClassicBucketsLifecycleSeconds: orders.lifecycle.duration (seconds):
	// 50ms .. 5m — the choreography SLO watches the <60s region.
	ClassicBucketsLifecycleSeconds = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 120, 300}
)

// HistogramViews returns the metric views applied to the MeterProvider that
// SetupAll builds (provider-wide → they affect BOTH the Prometheus-pull and
// the OTLP-push readers).
//
// classic=false (default): base-2 exponential histogram aggregation for every
// histogram-kind instrument whose name matches a duration/lag pattern.
//
// classic=true (TELEMETRY_CLASSIC_HISTOGRAMS=true): per-signal tuned explicit
// bucket tables — the documented fallback for native-histogram-less backends.
func HistogramViews(classic bool) []sdkmetric.View {
	if classic {
		return classicViews()
	}
	exp := sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, MaxScale: 20}
	views := make([]sdkmetric.View, 0, 2)
	for _, pattern := range []string{"*duration*", "*lag*"} {
		views = append(views, sdkmetric.NewView(
			sdkmetric.Instrument{Name: pattern, Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: exp},
		))
	}
	return views
}

// classicViews maps each known duration instrument to its tuned explicit
// bucket table. Wildcards keep future kafka duration instruments covered
// without editing this file.
func classicViews() []sdkmetric.View {
	tables := []struct {
		pattern string
		bounds  []float64
	}{
		{"http.server.duration", ClassicBucketsMillis},
		{"cqrs.handler.duration_ms", ClassicBucketsMillis},
		{"kafka.*duration", ClassicBucketsKafkaSeconds},
		{"outbox.publish_lag", ClassicBucketsOutboxSeconds},
		{"pg.query.duration", ClassicBucketsPgSeconds},
		{"orders.lifecycle.duration", ClassicBucketsLifecycleSeconds},
	}
	views := make([]sdkmetric.View, 0, len(tables))
	for _, t := range tables {
		views = append(views, sdkmetric.NewView(
			sdkmetric.Instrument{Name: t.pattern, Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: t.bounds}},
		))
	}
	return views
}
