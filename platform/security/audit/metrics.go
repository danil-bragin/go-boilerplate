package audit

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// writerMetrics bundles the BufferedAuditWriter instruments. Instruments are
// created from the GLOBAL otel meter (mirroring messaging/outbox and
// platform/cqrs); the global meter deduplicates identical instrument names. A
// failed instrument creation degrades to a nil instrument (no-op at call
// sites) — metrics must never break the audit path.
type writerMetrics struct {
	dropped metric.Int64Counter
	errors  metric.Int64Counter
}

func newWriterMetrics() writerMetrics {
	m := otel.Meter("security.audit")
	var wm writerMetrics
	if c, err := m.Int64Counter(
		"audit.dropped_total",
		metric.WithDescription("Async audit entries dropped because the buffer was full (Eventual-consistency flows)"),
	); err == nil {
		wm.dropped = c
	}
	if c, err := m.Int64Counter(
		"audit.async_write_errors_total",
		metric.WithDescription("Failed async audit batch writes"),
	); err == nil {
		wm.errors = c
	}
	return wm
}

func (m writerMetrics) addDropped(ctx context.Context) {
	if m.dropped != nil {
		m.dropped.Add(ctx, 1)
	}
}

func (m writerMetrics) addError(ctx context.Context) {
	if m.errors != nil {
		m.errors.Add(ctx, 1)
	}
}

// storeMetrics holds instruments used by the durable-audit drain path (PgStore
// methods). It is initialised exactly once via sync.Once so that multiple
// PgStore instances created in the same process share a single gauge
// registration — the global meter deduplicates by name, but one registration
// is cleaner. A failed instrument creation degrades to nil (no-op at call
// sites) matching the nil-degrading idiom above.
var (
	storeMetricsOnce sync.Once
	storeMetricsInst storeMetrics
)

type storeMetrics struct {
	pendingBacklog metric.Int64Gauge
}

func getStoreMetrics() storeMetrics {
	storeMetricsOnce.Do(func() {
		m := otel.Meter("security.audit")
		if g, err := m.Int64Gauge(
			"audit.pending_backlog",
			metric.WithDescription("Rows in audit_pending awaiting the durable-audit drain (lag indicator)"),
		); err == nil {
			storeMetricsInst.pendingBacklog = g
		}
	})
	return storeMetricsInst
}

// RecordPendingBacklog records the current number of rows in audit_pending as
// the audit.pending_backlog gauge. Call it from the drain worker each tick so
// operators can observe drain lag. Nil-degrading: if the gauge could not be
// registered at startup the call is a no-op and never returns an error.
func (s *PgStore) RecordPendingBacklog(ctx context.Context, n int64) {
	if g := getStoreMetrics().pendingBacklog; g != nil {
		g.Record(ctx, n)
	}
}
