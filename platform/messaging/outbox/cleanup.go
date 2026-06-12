package outbox

// Package-level doc note on scaling:
//
// This age-based cleaner is the "simple" mode default (OUTBOX_PARTITION_MODE=
// simple) and suits most deployments. At very high volume DELETE causes heap
// bloat and autovacuum pressure; switch to "partitioned" mode (ADR-0016), which
// DETACH+DROPs whole created_at partitions instead — see PartitionManager and
// servicekit.AddOutboxPartitionMaintenance. The table is RANGE-partitioned by
// created_at from day one, so that switch is a config flip, not a migration.

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/messaging/outbox/gen"
	"go-boilerplate/platform/storage/pg"

	"github.com/jackc/pgx/v5/pgtype"
)

// Cleaner performs age-based deletion of published outbox rows.
//
// Published rows accumulate without bound if never removed. Cleaner.Cleanup
// deletes rows where published_at is not null and older than olderThan.
//
// Intended usage: launch RunCleanup as a goroutine and register the returned
// cancel with run.Closer so it shuts down gracefully.
//
// For high-volume tables, prefer time-partitioning over age-based DELETE.
// See package-level documentation.
type Cleaner struct {
	pool    *pg.Pool
	onError func(error)
}

// NewCleaner returns a Cleaner backed by pool.
func NewCleaner(pool *pg.Pool) *Cleaner {
	return &Cleaner{pool: pool}
}

// SetOnError registers a callback invoked for each Cleanup error inside
// RunCleanup. The loop continues regardless of individual cleanup errors.
func (c *Cleaner) SetOnError(fn func(error)) {
	c.onError = fn
}

// Cleanup deletes published outbox rows older than olderThan from now.
// It returns the number of rows deleted.
func (c *Cleaner) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-olderThan)
	ts := pgtype.Timestamptz{Time: cutoff, Valid: true}
	q := gen.New(c.pool.Writer())
	n, err := q.DeletePublishedBefore(ctx, ts)
	if err != nil {
		return 0, fmt.Errorf("outbox: cleanup: %w", err)
	}
	return n, nil
}

// RunCleanup runs a ticker-based cleanup loop, calling Cleanup every interval.
// It returns ctx.Err() when the context is cancelled or times out.
// Individual cleanup errors are passed to the OnError hook (if set) and the
// loop continues.
//
// Intended to be launched as a goroutine and registered with run.Closer:
//
//	closer.Add(func() { cancel() })
//	go cleaner.RunCleanup(ctx, 1*time.Hour, 24*time.Hour)
func (c *Cleaner) RunCleanup(ctx context.Context, interval, retention time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := c.Cleanup(ctx, retention); err != nil {
				if c.onError != nil {
					c.onError(err)
				}
			}
		}
	}
}
