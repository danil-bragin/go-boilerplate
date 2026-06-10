package kafka

import (
	"context"
	"time"

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
	processed       metric.Int64Counter
	failed          metric.Int64Counter
	commitFailures  metric.Int64Counter
	lag             metric.Int64Gauge
	handlerDuration metric.Float64Histogram
}

func newConsumerMetrics() consumerMetrics {
	return newConsumerMetricsFrom(otel.Meter(meterName))
}

// newConsumerMetricsFrom creates the instruments from an explicit meter —
// the seam unit tests use to bind a private ManualReader provider without
// touching the otel global.
func newConsumerMetricsFrom(m metric.Meter) consumerMetrics {
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
	if h, err := m.Float64Histogram(
		"kafka.consumer.handler.duration",
		metric.WithDescription("Per-record handler invocation duration (excludes offset commit)"),
		metric.WithUnit("s"),
	); err == nil {
		cm.handlerDuration = h
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

// recordHandlerDuration observes one handler invocation in SECONDS with
// bounded labels {topic, status: ok|error}. Commit time is deliberately
// excluded — commits are batched per poll, not per record.
func (cm consumerMetrics) recordHandlerDuration(ctx context.Context, topic string, d time.Duration, err error) {
	if cm.handlerDuration == nil {
		return
	}
	status := "ok"
	if err != nil {
		status = "error"
	}
	cm.handlerDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("status", status),
	))
}

// producerMetrics bundles the producer-side instruments. Same construction
// rules as consumerMetrics: global meter, nil-degrade on creation failure.
type producerMetrics struct {
	publishDuration metric.Float64Histogram
}

func newProducerMetrics() producerMetrics {
	return newProducerMetricsFrom(otel.Meter(meterName))
}

// newProducerMetricsFrom creates the instruments from an explicit meter (test
// seam, see newConsumerMetricsFrom).
func newProducerMetricsFrom(m metric.Meter) producerMetrics {
	var pm producerMetrics
	if h, err := m.Float64Histogram(
		"kafka.producer.publish.duration",
		metric.WithDescription("Full synchronous publish round-trip until broker acknowledgment"),
		metric.WithUnit("s"),
	); err == nil {
		pm.publishDuration = h
	}
	return pm
}

// recordPublishDuration observes one synchronous publish RTT in SECONDS,
// labeled {topic} only. Recorded on success AND failure — a timed-out produce
// is exactly the tail latency this histogram exists to expose.
func (pm producerMetrics) recordPublishDuration(ctx context.Context, topic string, d time.Duration) {
	if pm.publishDuration == nil {
		return
	}
	pm.publishDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("topic", topic)))
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
