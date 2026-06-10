package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// localMeter returns a meter bound to a private ManualReader provider — the
// otel GLOBAL provider is never touched, so these unit tests cannot interfere
// with the package-external integration tests (which install a global reader).
func localMeter(t *testing.T) (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return reader, provider
}

// histDataPoints returns the float64-histogram datapoints of the named metric.
func histDataPoints(rm *metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
				return h.DataPoints
			}
		}
	}
	return nil
}

// attrValue returns the string value of key in the datapoint's attribute set.
func attrValue(dp metricdata.HistogramDataPoint[float64], key string) string {
	for _, kv := range dp.Attributes.ToSlice() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestConsumerMetrics_HandlerDuration verifies the per-record handler
// duration histogram: seconds unit, one datapoint per {topic, status} pair,
// status ok vs error split, and nil-instrument degradation.
func TestConsumerMetrics_HandlerDuration(t *testing.T) {
	t.Parallel()
	reader, provider := localMeter(t)
	cm := newConsumerMetricsFrom(provider.Meter(meterName))
	ctx := context.Background()

	cm.recordHandlerDuration(ctx, "orders.events", 250*time.Millisecond, nil)
	cm.recordHandlerDuration(ctx, "orders.events", 50*time.Millisecond, nil)
	cm.recordHandlerDuration(ctx, "orders.events", 10*time.Millisecond, errors.New("boom"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	dps := histDataPoints(&rm, "kafka.consumer.handler.duration")
	require.Len(t, dps, 2, "one datapoint per {topic,status} pair")

	byStatus := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, dp := range dps {
		require.Equal(t, "orders.events", attrValue(dp, "topic"))
		byStatus[attrValue(dp, "status")] = dp
	}

	ok, found := byStatus["ok"]
	require.True(t, found, "status=ok datapoint must exist")
	require.Equal(t, uint64(2), ok.Count)
	require.InDelta(t, 0.3, ok.Sum, 0.001, "values are recorded in SECONDS")

	bad, found := byStatus["error"]
	require.True(t, found, "status=error datapoint must exist")
	require.Equal(t, uint64(1), bad.Count)
	require.InDelta(t, 0.01, bad.Sum, 0.001)

	// Nil-instrument degradation must not panic.
	var zero consumerMetrics
	zero.recordHandlerDuration(ctx, "t", time.Second, nil)
}

// TestProducerMetrics_PublishDuration verifies the producer publish-RTT
// histogram: seconds unit, {topic} label, and nil-instrument degradation.
func TestProducerMetrics_PublishDuration(t *testing.T) {
	t.Parallel()
	reader, provider := localMeter(t)
	pm := newProducerMetricsFrom(provider.Meter(meterName))
	ctx := context.Background()

	pm.recordPublishDuration(ctx, "orders.events", 100*time.Millisecond)
	pm.recordPublishDuration(ctx, "payments.events", 2*time.Second)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	dps := histDataPoints(&rm, "kafka.producer.publish.duration")
	require.Len(t, dps, 2, "one datapoint per topic")

	byTopic := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, dp := range dps {
		byTopic[attrValue(dp, "topic")] = dp
	}
	require.InDelta(t, 0.1, byTopic["orders.events"].Sum, 0.001, "values are recorded in SECONDS")
	require.InDelta(t, 2.0, byTopic["payments.events"].Sum, 0.001)

	// Nil-instrument degradation must not panic.
	var zero producerMetrics
	zero.recordPublishDuration(ctx, "t", time.Second)
}

// TestMetrics_RecordPathAllocs: the hot record paths must not allocate once
// the per-topic attribute sets are cached — attribute construction is hoisted
// into a lazily-populated cache, and records pass the pre-built option slice.
// A noop meter isolates OUR path from SDK aggregation internals; the
// nil-instrument paths are asserted alloc-free as well.
func TestMetrics_RecordPathAllocs(t *testing.T) {
	// NOT parallel: testing.AllocsPerRun forbids parallel tests.
	ctx := context.Background()
	handlerErr := errors.New("boom")

	cm := newConsumerMetricsFrom(noop.NewMeterProvider().Meter(meterName))
	pm := newProducerMetricsFrom(noop.NewMeterProvider().Meter(meterName))

	// Warm the caches: the first record per topic (and per partition for lag)
	// is allowed to allocate the attribute sets.
	warm := func() {
		cm.recordHandlerDuration(ctx, "orders.events", time.Millisecond, nil)
		cm.recordHandlerDuration(ctx, "orders.events", time.Millisecond, handlerErr)
		cm.addProcessed(ctx, "orders.events", 1)
		cm.addFailed(ctx, "orders.events")
		cm.addCommitFailure(ctx, "orders.events")
		cm.recordLag(ctx, "orders.events", 3, 42)
		pm.recordPublishDuration(ctx, "orders.events", time.Millisecond)
	}
	warm()

	allocs := testing.AllocsPerRun(1000, warm)
	require.Zero(t, allocs, "cached record path must not allocate")

	// Nil-instrument (metrics disabled) paths must be alloc-free no-ops.
	var zeroCM consumerMetrics
	var zeroPM producerMetrics
	allocs = testing.AllocsPerRun(1000, func() {
		zeroCM.recordHandlerDuration(ctx, "t", time.Millisecond, nil)
		zeroCM.addProcessed(ctx, "t", 1)
		zeroCM.recordLag(ctx, "t", 0, 1)
		zeroPM.recordPublishDuration(ctx, "t", time.Millisecond)
	})
	require.Zero(t, allocs, "nil-instrument path must not allocate")
}
