package service

import (
	"time"

	"go-boilerplate/platform/log"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/telemetry"
)

// Config is the embeddable base config for all consumer services.
// Services embed this and add their own topic-name fields.
type Config struct {
	Log       log.Config
	Telemetry telemetry.Config
	PG        pg.Config
	Kafka     kafka.Config
	AdminAddr string `env:"ADMIN_HTTP_ADDR" envDefault:":9090"`

	// Inbox retention: processed inbox rows older than InboxRetention are
	// deleted every InboxCleanupInterval. Set InboxCleanupInterval to 0 to
	// disable inbox cleanup (e.g., when the service does not use the inbox
	// table — in practice all example services use it, so the default is safe).
	InboxRetention       time.Duration `env:"INBOX_RETENTION"         envDefault:"168h"`
	InboxCleanupInterval time.Duration `env:"INBOX_CLEANUP_INTERVAL"  envDefault:"1h"`

	// Audit log retention: audit_log rows older than AuditRetention are deleted
	// every AuditCleanupInterval by services that call AddAuditCleanup. Set
	// AuditCleanupInterval to 0 to disable audit cleanup for a given service.
	//
	// COMPLIANCE NOTE: audit logs often need long retention or archival to cold
	// storage before deletion. The default retention (90 days) is intentionally
	// generous. Review your compliance obligations before lowering it.
	AuditRetention       time.Duration `env:"AUDIT_RETENTION"         envDefault:"2160h"`
	AuditCleanupInterval time.Duration `env:"AUDIT_CLEANUP_INTERVAL"  envDefault:"6h"`
}
