package kafka

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName is the otel instrumentation scope for kafka messaging metrics.
const meterName = "messaging.kafka"

// consumerMetrics bundles the consumer-side instruments. Instruments are
// created from the GLOBAL otel meter at construction time (mirroring
// platform/cqrs Metrics); the global meter deduplicates identical instrument
// names, so every NewConsumer call may safely create its own set. A failed
// instrument creation degrades to a nil instrument (no-op at call sites) —
// metrics must never break message processing.
type consumerMetrics struct {
	processed      metric.Int64Counter
	failed         metric.Int64Counter
	commitFailures metric.Int64Counter
	lag            metric.Int64Gauge
}

func newConsumerMetrics() consumerMetrics {
	m := otel.Meter(meterName)
	var cm consumerMetrics
	if c, err := m.Int64Counter(
		"kafka.consumer.records_processed",
		metric.WithDescription("Records successfully processed by the handler"),
	); err == nil {
		cm.processed = c
	}
	if c, err := m.Int64Counter(
		"kafka.consumer.records_failed",
		metric.WithDescription("Records whose handler returned an error (will be redelivered)"),
	); err == nil {
		cm.failed = c
	}
	if c, err := m.Int64Counter(
		"kafka.consumer.commit_failures",
		metric.WithDescription("CommitRecords RPC failures (widen the duplicate-delivery window)"),
	); err == nil {
		cm.commitFailures = c
	}
	if g, err := m.Int64Gauge(
		"kafka.consumer.lag",
		metric.WithDescription("Consumer lag per partition: high watermark minus next offset to consume"),
	); err == nil {
		cm.lag = g
	}
	return cm
}

func (cm consumerMetrics) addProcessed(ctx context.Context, topic string, n int64) {
	if cm.processed == nil || n == 0 {
		return
	}
	cm.processed.Add(ctx, n, metric.WithAttributes(attribute.String("topic", topic)))
}

func (cm consumerMetrics) addFailed(ctx context.Context, topic string) {
	if cm.failed == nil {
		return
	}
	cm.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (cm consumerMetrics) addCommitFailure(ctx context.Context, topic string) {
	if cm.commitFailures == nil {
		return
	}
	cm.commitFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (cm consumerMetrics) recordLag(ctx context.Context, topic string, partition int32, lag int64) {
	if cm.lag == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	cm.lag.Record(ctx, lag, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
	))
}

// dltMetrics bundles the dead-letter-produce counter used by WithRetry.
type dltMetrics struct {
	produced metric.Int64Counter
}

func newDLTMetrics() dltMetrics {
	m := otel.Meter(meterName)
	var dm dltMetrics
	if c, err := m.Int64Counter(
		"kafka.dlt.produced",
		metric.WithDescription("Records routed to a dead-letter topic"),
	); err == nil {
		dm.produced = c
	}
	return dm
}

func (dm dltMetrics) addProduced(ctx context.Context, dltTopic string) {
	if dm.produced == nil {
		return
	}
	dm.produced.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", dltTopic)))
}
