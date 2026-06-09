package inbox

// Package-level scaling note:
//
// For very high volume, prefer time-partitioning (pg_partman / native
// PARTITION BY RANGE on processed_at) + DROP PARTITION over age-based DELETE.
// DELETE causes table bloat and autovacuum pressure at scale.
// This age-based cleaner is the simple default and is suitable for most
// deployments.

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// Cleanup deletes inbox rows whose processed_at is older than olderThan from
// now. It returns the number of rows deleted.
//
// The cutoff timestamp is computed in Go to avoid interval-binding issues with
// different Postgres driver versions.
func Cleanup(ctx context.Context, pool *pg.Pool, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-olderThan)
	tag, err := pool.Writer().Exec(
		ctx,
		`delete from inbox where processed_at < $1`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("inbox: cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunCleanup runs a ticker-based cleanup loop, calling Cleanup every interval
// with the given retention duration. It returns ctx.Err() when the context is
// cancelled or times out. Individual cleanup errors are passed to onError (if
// non-nil) and the loop continues.
//
// Intended to be launched as a goroutine and registered with run.Closer:
//
//	closer.Add(func() { cancel() })
//	go inbox.RunCleanup(ctx, pool, 1*time.Hour, 24*time.Hour)
func RunCleanup(ctx context.Context, pool *pg.Pool, interval, retention time.Duration) error {
	return RunCleanupWithOnError(ctx, pool, interval, retention, nil)
}

// RunCleanupWithOnError is RunCleanup with an optional error callback. The
// callback is invoked for each individual cleanup error; the loop continues
// regardless.
func RunCleanupWithOnError(ctx context.Context, pool *pg.Pool, interval, retention time.Duration, onError func(error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := Cleanup(ctx, pool, retention); err != nil {
				if onError != nil {
					onError(err)
				}
			}
		}
	}
}
