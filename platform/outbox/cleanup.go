package outbox

// Package-level doc note on scaling:
//
// For very high volume, prefer time-partitioning (pg_partman / native
// PARTITION BY RANGE on published_at) + DROP PARTITION over age-based DELETE.
// DELETE causes table bloat and autovacuum pressure at scale.
// This age-based cleaner is the simple default and is suitable for most
// deployments. Time-partitioning is the recommended path once table size
// becomes a bottleneck.

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"go-boilerplate/platform/outbox/gen"
	"go-boilerplate/platform/pg"
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
