package audit

// Package-level scaling note:
//
// For very high volume, prefer time-partitioning (pg_partman / native
// PARTITION BY RANGE on created_at) + DROP PARTITION over age-based DELETE.
// DELETE causes table bloat and autovacuum pressure at scale.
// This age-based cleaner is the simple default.
//
// COMPLIANCE WARNING: audit logs often need long retention or archival to cold
// storage before deletion. The default retention (90 days) is intentionally
// generous. Review your compliance obligations before lowering it. Consider
// archiving rows to object storage (e.g., S3) before deleting them.

import (
	"context"
	"fmt"
	"time"
)

// Cleanup deletes audit_log rows whose created_at is older than olderThan from
// now. It returns the number of rows deleted.
//
// Default retention should be generous (e.g., 90 days). Compliance
// requirements may mandate archival instead of deletion — see package note.
func (s *PgStore) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-olderThan)
	tag, err := s.pool.Writer().Exec(
		ctx,
		`delete from audit_log where created_at < $1`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("audit: cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunCleanup runs a ticker-based cleanup loop, calling Cleanup every interval
// with the given retention duration. It returns ctx.Err() when the context is
// cancelled or times out. Individual cleanup errors are passed to onError (if
// set via SetOnError) and the loop continues.
//
// Intended to be launched as a goroutine and registered with run.Closer:
//
//	closer.Add(func() { cancel() })
//	go store.RunCleanup(ctx, 1*time.Hour, 90*24*time.Hour)
func (s *PgStore) RunCleanup(ctx context.Context, interval, retention time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.Cleanup(ctx, retention); err != nil {
				if s.onError != nil {
					s.onError(err)
				}
			}
		}
	}
}
