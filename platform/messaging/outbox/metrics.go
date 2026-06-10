package outbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// relayMetrics bundles the relay instruments. Instruments are created from
// the GLOBAL otel meter at NewRelay time (mirroring platform/cqrs Metrics);
// the global meter deduplicates identical instrument names. A failed
// instrument creation degrades to a nil instrument (no-op at call sites) —
// metrics must never break publishing.
type relayMetrics struct {
	published     metric.Int64Counter
	publishErrors metric.Int64Counter
	pending       metric.Int64Gauge
	publishLag    metric.Float64Histogram
}

func newRelayMetrics() relayMetrics {
	m := otel.Meter("messaging.outbox")
	var rm relayMetrics
	if c, err := m.Int64Counter(
		"outbox.published",
		metric.WithDescription("Outbox rows successfully published and marked"),
	); err == nil {
		rm.published = c
	}
	if c, err := m.Int64Counter(
		"outbox.publish_errors",
		metric.WithDescription("Failed outbox publish batches (rows stay pending and are retried)"),
	); err == nil {
		rm.publishErrors = c
	}
	if g, err := m.Int64Gauge(
		"outbox.pending",
		metric.WithDescription("Unpublished outbox rows (publish backlog)"),
	); err == nil {
		rm.pending = g
	}
	if h, err := m.Float64Histogram(
		"outbox.publish_lag",
		metric.WithDescription("End-to-end DB insert → broker publish delay per row, including drain backlog"),
		metric.WithUnit("s"),
	); err == nil {
		rm.publishLag = h
	}
	return rm
}

func (rm relayMetrics) addPublished(ctx context.Context, n int64) {
	if rm.published == nil || n == 0 {
		return
	}
	rm.published.Add(ctx, n)
}

func (rm relayMetrics) addPublishError(ctx context.Context) {
	if rm.publishErrors == nil {
		return
	}
	rm.publishErrors.Add(ctx, 1)
}

// recordPublishLag observes one SECONDS sample per successfully published
// row: now − created_at. created_at is DATABASE time (DEFAULT now() at insert)
// while "now" is the APP clock, so the value carries DB↔app clock skew —
// negligible against the ≥ poll-interval magnitudes this histogram tracks,
// but the reason negative skew is clamped to zero rather than recorded.
func (rm relayMetrics) recordPublishLag(ctx context.Context, lag time.Duration) {
	if rm.publishLag == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	rm.publishLag.Record(ctx, lag.Seconds())
}

func (rm relayMetrics) recordPending(ctx context.Context, n int64) {
	if rm.pending == nil {
		return
	}
	rm.pending.Record(ctx, n)
}
