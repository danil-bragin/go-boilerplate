package servicekit

import (
	"context"
	"errors"
	"time"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/jackc/pgx/v5/pgxpool"
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
// APPEND-ONLY NOTE: migration 00003_audit_append_only revokes UPDATE/DELETE on
// audit_log from the app role, so the retention DELETE must run through a
// PRIVILEGED pool. When PG_AUDIT_ADMIN_URL is set this method dials it and
// registers it on the store via SetAdminPool; otherwise Cleanup is a no-op
// (audit.ErrCleanupDisabled) and the worker simply logs once-per-tick — the
// append-only guarantee always holds, rows just accumulate until an admin path
// (PG_AUDIT_ADMIN_URL or partition-drop) is wired.
//
// Must be called before Start.
func (s *Service) AddAuditCleanup(store *audit.PgStore, interval, retention time.Duration) {
	if interval <= 0 {
		return
	}
	// PHYSICAL SHARDING LIMITATION (Tier-3, ADR-0019): the audit_log lives in
	// every physical database, but the retention DELETE must run through a
	// PRIVILEGED admin pool (PG_AUDIT_ADMIN_URL), and only ONE admin DSN is
	// configured. The store is bound to shard 0's pool + that single admin
	// pool, so this worker can only prune shard 0's audit_log. We surface the
	// gap loudly rather than silently retaining the other shards' rows
	// forever; per-shard audit retention needs per-shard admin DSNs.
	if s.shards.Len() > 1 {
		s.logger.Warn(
			"servicekit: audit retention runs on physical shard 0 ONLY — PG_SHARDS>1 needs per-shard admin DSNs; "+
				"the other shards' audit_log rows are NOT pruned by this worker (append-only guarantee still holds)",
			"physical_shards", s.shards.Len(),
		)
	}
	if url := s.cfg.PG.AuditAdminURL.Reveal(); url != "" {
		if adminPool, err := pgxpool.New(context.Background(), url); err != nil {
			s.logger.Warn("servicekit: audit retention disabled — PG_AUDIT_ADMIN_URL pool failed", "error", err)
		} else {
			store.SetAdminPool(adminPool)
			s.closer.Add("audit-admin-pool", func(context.Context) error {
				adminPool.Close()
				return nil
			})
		}
	}
	// AddPeriodicWorker only errors on a non-positive interval (guarded
	// above) or singleActive without Postgres (singleActive is false here).
	_ = s.AddPeriodicWorker("audit-cleanup", interval, 0, false, func(ctx context.Context) error {
		_, err := store.Cleanup(ctx, retention)
		// Disabled cleanup (no admin pool) is the configured steady state, not
		// a worker failure — swallow it so the periodic worker doesn't error
		// every tick.
		if errors.Is(err, audit.ErrCleanupDisabled) {
			return nil
		}
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
	// Fan out over EVERY physical shard: the inbox lives in each database, so a
	// single-pool cleanup would only prune shard 0 and let the other shards'
	// inbox tables grow unbounded. The age-based DELETE is idempotent, so the
	// non-leader (singleActive=false) fan-out stays safe. M=1 ⇒ one shard,
	// identical to today. Same error-impossibility note as AddAuditCleanup.
	_ = s.AddPeriodicWorker("inbox-cleanup", s.cfg.InboxCleanupInterval, 0, false, func(ctx context.Context) error {
		return s.shards.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
			_, err := inbox.Cleanup(ctx, p, retention)
			return err
		})
	})
}
