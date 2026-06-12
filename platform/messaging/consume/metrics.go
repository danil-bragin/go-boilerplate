package consume

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// consumeMetrics bundles consume-pipeline instruments. Created from the global
// meter (mirroring platform/messaging/outbox); a failed instrument creation
// degrades to a nil instrument (no-op) — metrics must never break consumption.
type consumeMetrics struct {
	batchFallback metric.Int64Counter
}

func newConsumeMetrics() consumeMetrics {
	m := otel.Meter("messaging.consume")
	var cm consumeMetrics
	if c, err := m.Int64Counter(
		"consume.batch_fallback_total",
		metric.WithDescription("Batch-apply transactions that fell back to per-record processing (poison or runtime error)"),
	); err == nil {
		cm.batchFallback = c
	}
	return cm
}

func (cm consumeMetrics) addBatchFallback(ctx context.Context) {
	if cm.batchFallback != nil {
		cm.batchFallback.Add(ctx, 1)
	}
}
