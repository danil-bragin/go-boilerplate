package outbox

import (
	"context"

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

func (rm relayMetrics) recordPending(ctx context.Context, n int64) {
	if rm.pending == nil {
		return
	}
	rm.pending.Record(ctx, n)
}
