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
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrCleanupDisabled is returned by Cleanup when no privileged admin pool is
// configured (PG_AUDIT_ADMIN_URL unset). Migration 00003 revokes DELETE on
// audit_log from the app role, so retention can only run through a separate
// privileged pool; without one the call is a deliberate no-op.
var ErrCleanupDisabled = errors.New("audit: cleanup disabled (no PG_AUDIT_ADMIN_URL / admin pool configured)")

// SetAdminPool registers a privileged connection pool used for the retention
// DELETE. Migration 00003 REVOKEs UPDATE/DELETE on audit_log from the app
// role, so the app's own writer cannot delete; cleanup must run as a separate
// role that retains DELETE (deploy/postgres/init.sql provisions "audit_admin").
//
// Wire it from PG_AUDIT_ADMIN_URL at startup:
//
//	if url := cfg.AuditAdminURL; url != "" {
//	    p, _ := pgxpool.New(ctx, string(url))
//	    store.SetAdminPool(p)
//	}
//
// When no admin pool is set, Cleanup is a no-op that logs via onError: the
// REVOKE would block a DELETE on the app pool, so deleting through it is wrong
// (it errors) and silently skipping is safer than crashing the cleanup loop.
func (s *PgStore) SetAdminPool(p *pgxpool.Pool) {
	if p == nil {
		s.adminPool = nil
		return
	}
	s.adminPool = p
}

// Cleanup deletes audit_log rows whose created_at is older than olderThan from
// now. It returns the number of rows deleted.
//
// Retention runs through the privileged admin pool (SetAdminPool /
// PG_AUDIT_ADMIN_URL): migration 00003 revokes DELETE on audit_log from the
// app role, so a DELETE on the app writer is denied by design. With no admin
// pool configured, Cleanup is a no-op returning (0, ErrCleanupDisabled) — the
// append-only guarantee holds and rows simply accumulate until an admin path
// is wired (partition-drop or PG_AUDIT_ADMIN_URL).
//
// Default retention should be generous (e.g., 90 days). Compliance
// requirements may mandate archival instead of deletion — see package note.
func (s *PgStore) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	if s.adminPool == nil {
		return 0, ErrCleanupDisabled
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	tag, err := s.adminPool.Exec(
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
				// Disabled-cleanup is the configured steady state when no
				// admin pool is wired — surface it once-per-tick via onError
				// but never treat it as a hard failure.
				if s.onError != nil {
					s.onError(err)
				}
			}
		}
	}
}
