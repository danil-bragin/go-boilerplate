package servicekit

import (
	"time"

	"go-boilerplate/platform/config"
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

	// AdminBindOptional downgrades an admin-server bind failure in Start from
	// fatal error to a logged warning. Default false: a pod that cannot serve
	// /livez, /readyz and /metrics is invisible to orchestration and MUST NOT
	// come up half-alive. Set true only where that trade-off is understood
	// (e.g. ad-hoc local runs with a port squatter).
	AdminBindOptional bool `env:"ADMIN_BIND_OPTIONAL" envDefault:"false"`

	// MigrateOnStart controls whether New applies the service's embedded
	// migrations (advisory-locked, idempotent) before wiring anything else.
	// Default true — right for dev/test and single-team setups. In production
	// prefer a dedicated migrate job (run `cmd/migrate` or `just migrate <svc>`
	// in a pre-deploy step) and set MIGRATE_ON_START=false so app replicas
	// never race a long migration during rollout.
	MigrateOnStart bool `env:"MIGRATE_ON_START" envDefault:"true"`

	// PyroscopeAddr enables continuous profiling: when set (e.g.
	// "http://pyroscope:4040") the harness starts the grafana/pyroscope-go
	// profiler in New and stops it via the Closer. Empty (default) = no
	// profiler, zero overhead. The application name is Telemetry.ServiceName.
	PyroscopeAddr string `env:"PYROSCOPE_ADDR" envDefault:""`

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

	// In-process consumer retry knobs (used by AddConsumer's kafka.WithRetry
	// wrap: blocking attempts, then DLT). ConsumerRetryMaxAttempts is the
	// total number of handler invocations before the record dead-letters;
	// ConsumerRetryBackoff is the sleep between those attempts. Tiered retry
	// (AddConsumerWithRetry) is configured via retry.Policy instead.
	ConsumerRetryMaxAttempts int           `env:"CONSUMER_RETRY_MAX_ATTEMPTS" envDefault:"3"`
	ConsumerRetryBackoff     time.Duration `env:"CONSUMER_RETRY_BACKOFF"      envDefault:"100ms"`

	// SerdeSRURL enables Confluent Schema Registry wire-format framing of
	// outbox-published record values when set (e.g. "http://localhost:8081").
	// Unset (default) keeps raw protobuf values so the boilerplate runs
	// without a registry. Producers and consumers must agree: set it on both
	// sides of a topic or on neither.
	SerdeSRURL string `env:"SERDE_SR_URL" envDefault:""`

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

	// AuditChainKey keys the audit hash chain with HMAC-SHA256 instead of plain
	// sha256 (audit.WithChainKey). It is the difference between two security
	// guarantees:
	//
	//   - UNSET (default): the chain is keyless sha256. It detects EDIT and
	//     DELETE of existing rows (tamper-EVIDENCE), but does NOT resist FORGERY
	//     by the app role — the app owns audit_log and can INSERT, and sha256 is
	//     keyless, so a compromised app connection can append a row with a valid
	//     hash. Tamper-evidence here means "history cannot be silently
	//     rewritten", not "the app cannot fabricate history".
	//
	//   - SET (ideally injected from a secret manager and NOT readable by the
	//     app's DB role): the chain is HMAC-SHA256 keyed. An attacker holding
	//     only an app connection cannot compute a valid entry_hash for an
	//     appended row without the key, so forgery-by-append is caught by
	//     VerifyChain. This is the configuration required for forgery-resistance
	//     against a compromised application.
	//
	// Wire it into the store with audit.NewPgStore(pool,
	// audit.WithChainKey(cfg.AuditChainKey)).
	AuditChainKey config.Secret `env:"AUDIT_CHAIN_KEY" envDefault:""`

	// AuditChainShards splits the audit hash chain into N actor-keyed chains
	// (audit.WithChainShards, ADR-0018) so commands by different actors stop
	// serializing on a single chain-head lock. 1 (default) = one global chain,
	// behaviour unchanged. Raise it toward the number of distinct hot actors
	// when the global audit-chain lock becomes the command-throughput ceiling.
	AuditChainShards int `env:"AUDIT_CHAIN_SHARDS" envDefault:"1"`
}

// RetentionInvariantViolated reports whether the inbox dedup window is shorter
// than the broker's topic-retention horizon — the replay-after-cleanup hole. A
// record redelivered after its inbox row was cleaned up but before topic
// retention expired is no longer recognized as a duplicate. Only meaningful
// when TopicRetention > 0 (0 = "let the broker default decide", not asserted).
func (c Config) RetentionInvariantViolated() bool {
	return c.TopicRetention > 0 && c.InboxRetention < c.TopicRetention
}
