package kafka

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName is the otel instrumentation scope for kafka messaging metrics.
const meterName = "messaging.kafka"

// Attribute-set caching: the record paths below run once per record (or per
// poll for lag), so they must not rebuild attribute.Set values on every call.
// Sets are pre-encoded into ready-to-pass option slices and cached lazily in
// sync.Maps — cardinality is bounded (topics a consumer subscribes to /
// producer publishes to; topic×partition for lag). Records pass the cached
// slice via variadic expansion, so the hot path is allocation-free (verified
// by TestMetrics_RecordPathAllocs).

// consumerTopicAttrs bundles the pre-encoded per-topic measurement options.
type consumerTopicAttrs struct {
	add []metric.AddOption    // {topic}
	ok  []metric.RecordOption // {topic, status="ok"}
	err []metric.RecordOption // {topic, status="error"}
}

// topicPartition is the lag-attribute cache key.
type topicPartition struct {
	topic     string
	partition int32
}

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

	topicAttrs *sync.Map // string(topic) → *consumerTopicAttrs
	lagAttrs   *sync.Map // topicPartition → []metric.RecordOption
}

func newConsumerMetrics() consumerMetrics {
	return newConsumerMetricsFrom(otel.Meter(meterName))
}

// newConsumerMetricsFrom creates the instruments from an explicit meter —
// the seam unit tests use to bind a private ManualReader provider without
// touching the otel global.
func newConsumerMetricsFrom(m metric.Meter) consumerMetrics {
	cm := consumerMetrics{
		topicAttrs: &sync.Map{},
		lagAttrs:   &sync.Map{},
	}
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

// topicOpts returns the cached per-topic options, building them on first use.
func (cm consumerMetrics) topicOpts(topic string) *consumerTopicAttrs {
	if v, ok := cm.topicAttrs.Load(topic); ok {
		return v.(*consumerTopicAttrs) //nolint:forcetypeassert // cache stores *consumerTopicAttrs only
	}
	a := &consumerTopicAttrs{
		add: []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(
			attribute.String("topic", topic),
		))},
		ok: []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(
			attribute.String("topic", topic),
			attribute.String("status", "ok"),
		))},
		err: []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(
			attribute.String("topic", topic),
			attribute.String("status", "error"),
		))},
	}
	v, _ := cm.topicAttrs.LoadOrStore(topic, a)
	return v.(*consumerTopicAttrs) //nolint:forcetypeassert // cache stores *consumerTopicAttrs only
}

// lagOpts returns the cached {topic, partition} options. Partition cardinality
// is bounded by the topic's partition count, so the cache stays small.
func (cm consumerMetrics) lagOpts(topic string, partition int32) []metric.RecordOption {
	key := topicPartition{topic: topic, partition: partition}
	if v, ok := cm.lagAttrs.Load(key); ok {
		return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
	}
	opts := []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
	))}
	v, _ := cm.lagAttrs.LoadOrStore(key, opts)
	return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
}

func (cm consumerMetrics) addProcessed(ctx context.Context, topic string, n int64) {
	if cm.processed == nil || n == 0 {
		return
	}
	cm.processed.Add(ctx, n, cm.topicOpts(topic).add...)
}

func (cm consumerMetrics) addFailed(ctx context.Context, topic string) {
	if cm.failed == nil {
		return
	}
	cm.failed.Add(ctx, 1, cm.topicOpts(topic).add...)
}

func (cm consumerMetrics) addCommitFailure(ctx context.Context, topic string) {
	if cm.commitFailures == nil {
		return
	}
	cm.commitFailures.Add(ctx, 1, cm.topicOpts(topic).add...)
}

func (cm consumerMetrics) recordLag(ctx context.Context, topic string, partition int32, lag int64) {
	if cm.lag == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	cm.lag.Record(ctx, lag, cm.lagOpts(topic, partition)...)
}

// recordHandlerDuration observes one handler invocation in SECONDS with
// bounded labels {topic, status: ok|error}. Commit time is deliberately
// excluded — commits are batched per poll, not per record.
func (cm consumerMetrics) recordHandlerDuration(ctx context.Context, topic string, d time.Duration, err error) {
	if cm.handlerDuration == nil {
		return
	}
	opts := cm.topicOpts(topic)
	statusOpts := opts.ok
	if err != nil {
		statusOpts = opts.err
	}
	cm.handlerDuration.Record(ctx, d.Seconds(), statusOpts...)
}

// producerMetrics bundles the producer-side instruments. Same construction
// rules as consumerMetrics: global meter, nil-degrade on creation failure.
type producerMetrics struct {
	publishDuration metric.Float64Histogram

	topicAttrs *sync.Map // string(topic) → []metric.RecordOption
}

func newProducerMetrics() producerMetrics {
	return newProducerMetricsFrom(otel.Meter(meterName))
}

// newProducerMetricsFrom creates the instruments from an explicit meter (test
// seam, see newConsumerMetricsFrom).
func newProducerMetricsFrom(m metric.Meter) producerMetrics {
	pm := producerMetrics{topicAttrs: &sync.Map{}}
	if h, err := m.Float64Histogram(
		"kafka.producer.publish.duration",
		metric.WithDescription("Full synchronous publish round-trip until broker acknowledgment"),
		metric.WithUnit("s"),
	); err == nil {
		pm.publishDuration = h
	}
	return pm
}

// publishDurationEnabled reports whether the publish-duration instrument is
// live — callers that aggregate inputs solely for this metric (ProduceBatch's
// distinct-topic set) can skip that work entirely when it is nil-degraded.
func (pm producerMetrics) publishDurationEnabled() bool {
	return pm.publishDuration != nil
}

// topicOpts returns the cached per-topic options, building them on first use.
func (pm producerMetrics) topicOpts(topic string) []metric.RecordOption {
	if v, ok := pm.topicAttrs.Load(topic); ok {
		return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
	}
	opts := []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(
		attribute.String("topic", topic),
	))}
	v, _ := pm.topicAttrs.LoadOrStore(topic, opts)
	return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
}

// recordPublishDuration observes one synchronous publish RTT in SECONDS,
// labeled {topic} only. Recorded on success AND failure — a timed-out produce
// is exactly the tail latency this histogram exists to expose.
func (pm producerMetrics) recordPublishDuration(ctx context.Context, topic string, d time.Duration) {
	if pm.publishDuration == nil {
		return
	}
	pm.publishDuration.Record(ctx, d.Seconds(), pm.topicOpts(topic)...)
}

// dltMetrics bundles the dead-letter-produce counter used by WithRetry.
type dltMetrics struct {
	produced metric.Int64Counter

	topicAttrs *sync.Map // string(DLT topic) → []metric.AddOption
}

func newDLTMetrics() dltMetrics {
	m := otel.Meter(meterName)
	dm := dltMetrics{topicAttrs: &sync.Map{}}
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
	if v, ok := dm.topicAttrs.Load(dltTopic); ok {
		dm.produced.Add(ctx, 1, v.([]metric.AddOption)...) //nolint:forcetypeassert // cache stores []metric.AddOption only
		return
	}
	opts := []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(
		attribute.String("topic", dltTopic),
	))}
	v, _ := dm.topicAttrs.LoadOrStore(dltTopic, opts)
	dm.produced.Add(ctx, 1, v.([]metric.AddOption)...) //nolint:forcetypeassert // cache stores []metric.AddOption only
}
