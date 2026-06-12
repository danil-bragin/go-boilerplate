package outbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// partitionMetrics bundles the partition-manager instruments. Created from the
// global meter (like relayMetrics); a failed creation degrades to a nil
// instrument (no-op) — maintenance metrics must never break the worker.
type partitionMetrics struct {
	count       metric.Int64Gauge
	oldestBound metric.Int64Gauge
	newestBound metric.Int64Gauge
	created     metric.Int64Counter
	dropped     metric.Int64Counter
	skipped     metric.Int64Counter
	runDuration metric.Float64Histogram
}

func newPartitionMetrics() partitionMetrics {
	m := otel.Meter("messaging.outbox")
	var pm partitionMetrics
	if g, err := m.Int64Gauge("outbox.partitions",
		metric.WithDescription("Number of time-range outbox partitions (excludes DEFAULT)")); err == nil {
		pm.count = g
	}
	if g, err := m.Int64Gauge("outbox.partition_oldest_bound_seconds",
		metric.WithDescription("Unix-seconds lower bound of the oldest outbox partition")); err == nil {
		pm.oldestBound = g
	}
	if g, err := m.Int64Gauge("outbox.partition_newest_bound_seconds",
		metric.WithDescription("Unix-seconds upper bound of the newest outbox partition")); err == nil {
		pm.newestBound = g
	}
	if c, err := m.Int64Counter("outbox.partitions_created",
		metric.WithDescription("Outbox partitions ensured/created by the maintenance worker")); err == nil {
		pm.created = c
	}
	if c, err := m.Int64Counter("outbox.partitions_dropped",
		metric.WithDescription("Expired outbox partitions DETACH+DROPped")); err == nil {
		pm.dropped = c
	}
	if c, err := m.Int64Counter("outbox.partitions_drop_skipped_unpublished",
		metric.WithDescription("Expired partitions left in place because they still held unpublished rows")); err == nil {
		pm.skipped = c
	}
	if h, err := m.Float64Histogram("outbox.partition_maintenance_duration",
		metric.WithDescription("Duration of one partition maintenance cycle"),
		metric.WithUnit("s")); err == nil {
		pm.runDuration = h
	}
	return pm
}

func (pm partitionMetrics) recordCount(ctx context.Context, n int64) {
	if pm.count != nil {
		pm.count.Record(ctx, n)
	}
}

func (pm partitionMetrics) recordBounds(ctx context.Context, oldest, newest time.Time) {
	if pm.oldestBound != nil {
		pm.oldestBound.Record(ctx, oldest.Unix())
	}
	if pm.newestBound != nil {
		pm.newestBound.Record(ctx, newest.Unix())
	}
}

func (pm partitionMetrics) addCreated(ctx context.Context) {
	if pm.created != nil {
		pm.created.Add(ctx, 1)
	}
}

func (pm partitionMetrics) addDropped(ctx context.Context) {
	if pm.dropped != nil {
		pm.dropped.Add(ctx, 1)
	}
}

func (pm partitionMetrics) addSkipped(ctx context.Context) {
	if pm.skipped != nil {
		pm.skipped.Add(ctx, 1)
	}
}

func (pm partitionMetrics) recordRun(ctx context.Context, d time.Duration) {
	if pm.runDuration != nil {
		pm.runDuration.Record(ctx, d.Seconds())
	}
}
