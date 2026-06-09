package servicekit

import (
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/observability/telemetry"
	"go-boilerplate/platform/storage/pg"
)

// Config is the embeddable base config for all consumer services.
// Services embed this and add their own topic-name fields.
type Config struct {
	Log       log.Config
	Telemetry telemetry.Config
	PG        pg.Config
	Kafka     kafka.Config
	AdminAddr string `env:"ADMIN_HTTP_ADDR" envDefault:":9090"`

	// DrainGrace is how long the service keeps running AFTER flipping /readyz
	// to 503 and BEFORE any teardown begins. This gives load balancers time to
	// observe the not-ready state and stop routing new traffic, so in-flight
	// requests/records drain instead of being cut off. Set to 0 to skip the
	// grace sleep (useful in tests).
	DrainGrace time.Duration `env:"DRAIN_GRACE" envDefault:"5s"`

	// Topic provisioning (used by Service.EnsureTopics, which AddConsumer and
	// AddConsumerWithRetry call for every topic they wire):
	//
	//   - TopicPartitions: partition count for newly-created topics. Bounds
	//     consumer-group parallelism; default 6 gives headroom to scale a
	//     group to 6 members without repartitioning.
	//   - TopicRF: replication factor. Default 1 suits local single-broker
	//     development ONLY — production clusters should run RF ≥ 3.
	//   - TopicRetention: applied as retention.ms on created topics. Set it
	//     explicitly and keep the invariant InboxRetention ≥ TopicRetention:
	//     the inbox dedup window must cover the broker's redelivery horizon,
	//     otherwise an old record redelivered near the retention edge is no
	//     longer recognized as a duplicate. New logs a startup WARN when the
	//     invariant is violated.
	//   - EnsureTopics: when false, Service.EnsureTopics is a no-op. Default
	//     true for dev/test bootstrap; in production manage topics as IaC and
	//     set ENSURE_TOPICS=false.
	TopicPartitions int32         `env:"TOPIC_PARTITIONS" envDefault:"6"`
	TopicRF         int16         `env:"TOPIC_RF"         envDefault:"1"`
	TopicRetention  time.Duration `env:"TOPIC_RETENTION"  envDefault:"168h"`
	EnsureTopics    bool          `env:"ENSURE_TOPICS"    envDefault:"true"`

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
