package servicekit

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/security/audit"
)

// AddAuditCleanup registers a periodic worker that deletes old audit_log rows
// under the service lifecycle. Call this after building the *audit.PgStore
// for a service that writes audit entries (e.g., orders, payments).
//
// The cleanup runs every interval and removes rows older than retention.
// Set interval to 0 to skip launching (useful when auditing is disabled).
//
// The worker runs on EVERY instance (singleActive=false): the age-based
// DELETE is idempotent, so concurrent replicas just delete disjoint or
// already-gone rows — no leader election needed.
//
// COMPLIANCE NOTE: audit_log rows record sensitive business actions. The
// default retention (90 days) is intentionally generous. Review your compliance
// obligations before lowering it — consider archiving rows to cold storage
// before deletion.
//
// Must be called before Start.
func (s *Service) AddAuditCleanup(store *audit.PgStore, interval, retention time.Duration) {
	if interval <= 0 {
		return
	}
	// AddPeriodicWorker only errors on a non-positive interval (guarded
	// above) or singleActive without Postgres (singleActive is false here).
	_ = s.AddPeriodicWorker("audit-cleanup", interval, 0, false, func(ctx context.Context) error {
		_, err := store.Cleanup(ctx, retention)
		return err
	})
}

// registerInboxCleanup registers the inbox-row cleanup as a periodic worker
// if configured. Called from Start, before the goroutine launch loop.
//
// Like every periodic worker the goroutine is tracked by s.wg: the
// consumers-cancel closer cancels runCtx and WAITS on s.wg before the pg
// closer runs, so an in-flight cleanup DELETE can never race pool teardown.
//
// Runs on EVERY instance (singleActive=false): the age-based DELETE is
// idempotent, so concurrent replicas are safe without leader election.
func (s *Service) registerInboxCleanup() {
	if s.cfg.InboxCleanupInterval <= 0 || s.pool == nil {
		return
	}
	retention := s.cfg.InboxRetention
	if retention == 0 {
		retention = 168 * time.Hour // 7d fallback
	}
	pool := s.pool
	// Same error-impossibility note as AddAuditCleanup.
	_ = s.AddPeriodicWorker("inbox-cleanup", s.cfg.InboxCleanupInterval, 0, false, func(ctx context.Context) error {
		_, err := inbox.Cleanup(ctx, pool, retention)
		return err
	})
}
