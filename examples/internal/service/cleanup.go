package service

import (
	"context"
	"time"

	"go-boilerplate/platform/audit"
	"go-boilerplate/platform/inbox"
)

// AddAuditCleanup registers a goroutine that deletes old audit_log rows under
// the service lifecycle. Call this after building the *audit.PgStore for a
// service that writes audit entries (e.g., orders, payments).
//
// The cleanup loop runs every interval and removes rows older than retention.
// Set interval to 0 to skip launching (useful when auditing is disabled).
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
	store.SetOnError(func(err error) {
		s.logger.Error("audit cleaner error", "error", err)
	})
	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := store.RunCleanup(ctx, interval, retention); err != nil && ctx.Err() == nil {
			s.logger.Error("audit cleaner stopped unexpectedly", "error", err)
		}
	})
}

// startInboxCleanup launches the inbox-row cleanup goroutine if configured.
// Called from Start; runCtx is the goroutine lifetime context.
func (s *Service) startInboxCleanup(runCtx context.Context) {
	if s.cfg.InboxCleanupInterval <= 0 {
		return
	}
	retention := s.cfg.InboxRetention
	if retention == 0 {
		retention = 168 * time.Hour // 7d fallback
	}
	interval := s.cfg.InboxCleanupInterval
	go func() {
		if err := inbox.RunCleanupWithOnError(runCtx, s.pool, interval, retention, func(err error) {
			s.logger.Error("inbox cleaner error", "error", err)
		}); err != nil && runCtx.Err() == nil {
			s.logger.Error("inbox cleaner stopped unexpectedly", "error", err)
		}
	}()
}
