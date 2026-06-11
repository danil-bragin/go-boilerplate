package gateway

import (
	"time"

	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/storage/cache"
	"go-boilerplate/platform/web/httpserver"
)

// Config aggregates all configuration for the gateway service.
type Config struct {
	servicekit.Config
	HTTP                httpserver.Config
	Cache               cache.Config
	S3                  blob.Config
	CommandsTopic       string `env:"ORDERS_COMMANDS_TOPIC"        envDefault:"orders.commands"`
	OrdersEventsTopic   string `env:"ORDERS_EVENTS_TOPIC"   envDefault:"orders.events"`
	PaymentsEventsTopic string `env:"PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
	AuthDisabled        bool   `env:"GATEWAY_AUTH_DISABLED"         envDefault:"false"`
	JWKSUrl             string `env:"GATEWAY_JWKS_URL"              envDefault:""`
	JWKSIssuer          string `env:"GATEWAY_JWKS_ISSUER"           envDefault:""`
	JWKSAudience        string `env:"GATEWAY_JWKS_AUDIENCE"         envDefault:""`
	// AuthClockSkew is the acceptable clock skew for exp/iat/nbf validation —
	// tolerates issuer/verifier clock drift (jwt.WithAcceptableSkew).
	AuthClockSkew time.Duration `env:"AUTH_CLOCK_SKEW" envDefault:"30s"`
	// AuthRequiredAZP, when set, requires the token's azp (authorized party)
	// claim to match — pins tokens to the OAuth client they were issued to.
	// Empty disables the check.
	AuthRequiredAZP string `env:"AUTH_REQUIRED_AZP" envDefault:""`
	// AuthMaxTokenBytes caps the Bearer-token size the auth middleware will
	// even attempt to verify: a token longer than this is rejected with 401
	// by a cheap len-check BEFORE jwt.Parse, so an oversized Authorization
	// header cannot force per-request signature/parse work. Default 8192
	// comfortably holds a fat Keycloak token; a non-positive value falls back
	// to the middleware default.
	AuthMaxTokenBytes int `env:"AUTH_MAX_TOKEN_BYTES" envDefault:"8192"`
	// CORSOrigins is the list of allowed CORS origins for the public HTTP server.
	// Default empty = DENY ALL cross-origin browser requests (no ACAO header
	// emitted, preflights rejected). Set explicit origins in production, or
	// "*" for local dev/demo only — never "*" with credentialed requests.
	CORSOrigins []string `env:"GATEWAY_CORS_ORIGINS"          envSeparator:"," envDefault:""`
	// RatelimitRPS is the sustained token refill rate (requests per second per IP).
	RatelimitRPS float64 `env:"RATELIMIT_RPS"   envDefault:"50"`
	// RatelimitBurst is the maximum burst depth per IP.
	RatelimitBurst int `env:"RATELIMIT_BURST" envDefault:"100"`
	// TrustedProxies is a comma-separated list of CIDR prefixes whose X-Forwarded-For
	// headers are trusted for client-IP extraction. Empty = RemoteAddr only.
	TrustedProxies []string `env:"TRUSTED_PROXIES" envSeparator:","`
	// RatelimitRedis enables a Redis-backed distributed limiter when true.
	// Falls back to in-memory if Redis is unavailable (graceful degradation).
	// Applies to BOTH the per-IP and the authed-tier limiter.
	RatelimitRedis bool `env:"RATELIMIT_REDIS" envDefault:"false"`
	// RatelimitAuthedRPS is the sustained refill rate of the SECOND, authed-tier
	// limiter, keyed per principal (token subject; anonymous requests fall back
	// to the client IP). Chained AFTER the per-IP limiter — both must pass.
	// 0 disables the authed tier entirely.
	RatelimitAuthedRPS float64 `env:"RATELIMIT_AUTHED_RPS" envDefault:"200"`
	// RatelimitAuthedBurst is the burst depth of the authed-tier limiter.
	RatelimitAuthedBurst int `env:"RATELIMIT_AUTHED_BURST" envDefault:"400"`
	// SSEHeartbeat is the keep-alive comment interval for SSE streams
	// (GET /v1/orders/{id}/events). Keep it well below any intermediary's
	// idle-connection timeout.
	SSEHeartbeat time.Duration `env:"GATEWAY_SSE_HEARTBEAT" envDefault:"15s"`
	// SSEPollInterval is the projection-store polling cadence SSE falls back
	// to when REDIS_ADDRS is not configured (no pub/sub push available).
	SSEPollInterval time.Duration `env:"GATEWAY_SSE_POLL_INTERVAL" envDefault:"2s"`
	// SSEMaxStreams caps concurrently open SSE streams per replica (bulkhead
	// guarding the per-stream memory/FD surface). When the cap is reached a
	// NEW stream gets 503 GATEWAY_SSE_SATURATED (+ small Retry-After); the
	// permit frees as soon as any stream ends. 0 (default) = no cap.
	SSEMaxStreams int `env:"GATEWAY_SSE_MAX_STREAMS" envDefault:"0"`
	// EmbeddedProjection controls whether this gateway process runs the
	// read-model projection consumer (default true — single-binary demo
	// topology). Set false when the projection runs as its own deployment
	// (examples/gateway/cmd/projection): the gateway then serves HTTP only
	// and the standalone binary owns the consumer group. Both modes share the
	// same consumer group ("gateway-projection") and inbox table, so a
	// rolling migration between modes is safe.
	EmbeddedProjection bool `env:"GATEWAY_EMBEDDED_PROJECTION" envDefault:"true"`
	// PendingAsync moves the POST-time pending-row insert off the request
	// path into a single batched async writer (api.PendingBatcher: one
	// multi-row INSERT ... ON CONFLICT DO NOTHING per ≤50ms/≤100 rows).
	// Default false keeps the insert synchronous — read-your-writes UX: an
	// immediate GET after POST sees the pending row. true trades that for
	// writer relief under burst: GET right after POST may 404 until the
	// batch flushes; an Idempotency-Key body-mismatch within the flush
	// window returns 202 instead of 409 (the mismatch check reads the
	// not-yet-flushed row — detection resumes once the projection writes
	// it); rows are best-effort (dropped + WARN +
	// gateway.pending_async.dropped counter on buffer-full AND on failed
	// batch INSERTs — the projection creates the row when OrderCreated
	// arrives regardless).
	PendingAsync bool `env:"GATEWAY_PENDING_ASYNC" envDefault:"false"`
}
