package kafka_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricsReader installs a manual-reader SDK provider as the otel global,
// exactly once per test binary (instruments created from the global meter
// bind to the first provider set).
var (
	metricsReaderOnce sync.Once
	metricsReader     *sdkmetric.ManualReader
)

func manualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	metricsReaderOnce.Do(func() {
		metricsReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricsReader)))
	})
	return metricsReader
}

// sumCounter returns the summed int64 datapoints of the named counter.
func sumCounter(rm *metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// hasGauge reports whether the named int64 gauge has at least one datapoint.
func hasGauge(rm *metricdata.ResourceMetrics, name string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) > 0 {
				return true
			}
		}
	}
	return false
}

// TestConsumerMetrics_ProcessedFailedLag verifies the consumer-side metrics:
// records_processed and records_failed counters move, and the lag gauge is
// populated from fetch high-watermarks.
func TestConsumerMetrics_ProcessedFailedLag(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	reader := manualReader(t)

	broker, _ := kafkatest.NewRedpanda(t)
	ctx := context.Background()
	topic := "metrics-topic-" + uuid.NewString()[:8]

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "metrics-admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	prod := kafka.NewProducer(adminCl)
	for _, v := range []string{"r1", "r2", "r3"} {
		require.NoError(t, prod.Produce(ctx, kafka.Record{Topic: topic, Key: []byte(v), Value: []byte(v)}))
	}

	var processed atomic.Int64
	var failedOnce atomic.Bool
	handler := func(_ context.Context, r kafka.Record) error {
		if string(r.Value) == "r2" && !failedOnce.Load() {
			failedOnce.Store(true)
			return errors.New("transient failure on r2")
		}
		processed.Add(1)
		return nil
	}

	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "metrics-consumer",
		GroupID:  "metrics-group-" + uuid.NewString()[:8],
	}, []string{topic})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = consumer.Run(runCtx, handler) }()

	require.Eventually(t, func() bool { return processed.Load() == 3 },
		30*time.Second, 50*time.Millisecond, "all 3 records must be processed")
	cancel()
	<-done
	require.NoError(t, consumer.Close(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	require.GreaterOrEqual(t, sumCounter(&rm, "kafka.consumer.records_processed"), int64(3),
		"records_processed counter must count successful records")
	require.GreaterOrEqual(t, sumCounter(&rm, "kafka.consumer.records_failed"), int64(1),
		"records_failed counter must count handler failures")
	require.True(t, hasGauge(&rm, "kafka.consumer.lag"),
		"consumer lag gauge must be populated from fetches")
}

// TestDLTMetrics_ProducedCounter verifies kafka.dlt.produced moves when
// WithRetry routes a poison record to the DLT.
func TestDLTMetrics_ProducedCounter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	reader := manualReader(t)

	broker, _ := kafkatest.NewRedpanda(t)
	ctx := context.Background()
	topic := "dltm-topic-" + uuid.NewString()[:8]

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "dltm-admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl,
		kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic, topic+".DLT"))

	prod := kafka.NewProducer(adminCl)
	require.NoError(t, prod.Produce(ctx, kafka.Record{Topic: topic, Key: []byte("k"), Value: []byte("poison")}))

	var dltDone atomic.Bool
	wrapped := kafka.WithRetry(func(_ context.Context, _ kafka.Record) error {
		return errors.New("always fails")
	}, kafka.RetryOpts{MaxAttempts: 1, Producer: prod, Backoff: 10 * time.Millisecond})

	tracking := func(ctx context.Context, r kafka.Record) error {
		err := wrapped(ctx, r)
		if err == nil {
			dltDone.Store(true) // nil after exhausted attempts ⇒ DLT produced
		}
		return err
	}

	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "dltm-consumer",
		GroupID:  "dltm-group-" + uuid.NewString()[:8],
	}, []string{topic})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = consumer.Run(runCtx, tracking) }()

	require.Eventually(t, dltDone.Load,
		30*time.Second, 50*time.Millisecond, "poison record must reach the DLT")
	cancel()
	<-done
	require.NoError(t, consumer.Close(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	require.GreaterOrEqual(t, sumCounter(&rm, "kafka.dlt.produced"), int64(1),
		"dlt.produced counter must count DLT writes")
}
