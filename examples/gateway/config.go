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
	RatelimitRedis bool `env:"RATELIMIT_REDIS" envDefault:"false"`
	// EmbeddedProjection controls whether this gateway process runs the
	// read-model projection consumer (default true — single-binary demo
	// topology). Set false when the projection runs as its own deployment
	// (examples/gateway/cmd/projection): the gateway then serves HTTP only
	// and the standalone binary owns the consumer group. Both modes share the
	// same consumer group ("gateway-projection") and inbox table, so a
	// rolling migration between modes is safe.
	EmbeddedProjection bool `env:"GATEWAY_EMBEDDED_PROJECTION" envDefault:"true"`
}
