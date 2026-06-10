package projection

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// lifecycleMetrics bundles the business-level order lifecycle instrument.
// Created from the GLOBAL otel meter at NewHandler time (mirroring the
// platform messaging metrics); a failed instrument creation degrades to a
// nil instrument (no-op) — metrics must never break projection writes.
type lifecycleMetrics struct {
	duration metric.Float64Histogram
}

func newLifecycleMetrics() lifecycleMetrics {
	var lm lifecycleMetrics
	if h, err := otel.Meter("gateway.projection").Float64Histogram(
		"orders.lifecycle.duration",
		metric.WithDescription("Order creation → terminal status latency, observed once per applied terminal write"),
		metric.WithUnit("s"),
	); err == nil {
		lm.duration = h
	}
	return lm
}

// observe records one SECONDS sample of now − createdAt under the bounded
// terminal_status label (paid|payment_failed|payment_timeout).
//
// Called ONLY when the terminal upsert APPLIED (RETURNING created_at yielded
// a row) — duplicate/reordered terminal events return no row and never reach
// here, so an order is observed exactly once. Two caveats, both inherent to
// at-least-once consumption: (1) the sample is recorded inside the inbox
// transaction, so a commit failure after the observe double-counts on the
// redelivery; (2) createdAt is DATABASE time while "now" is the app clock, so
// the value carries DB↔app skew — negative skew clamps to zero. When the
// terminal event arrives BEFORE OrderCreated (reorder), the upsert's INSERT
// arm creates a placeholder row whose created_at is unknown territory — the
// caller detects that via the query's `(xmax = 0) AS inserted` column and
// skips the observation entirely (a ≈0s sample would lie compliant and bias
// the SLO-2 good leg).
func (lm lifecycleMetrics) observe(ctx context.Context, terminalStatus string, createdAt time.Time) {
	if lm.duration == nil {
		return
	}
	elapsed := time.Now().UTC().Sub(createdAt)
	if elapsed < 0 {
		elapsed = 0
	}
	lm.duration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(
		attribute.String("terminal_status", terminalStatus),
	))
}
