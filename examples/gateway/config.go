package gateway

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"go-boilerplate/platform/config"
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
	// AuthAllowInsecureJWKS permits a non-https (http://) JWKS URL. Default
	// false: the verifier refuses a plaintext JWKS URL at startup (fail
	// closed — http keys can be MITM-swapped to forge tokens). Compose dev
	// sets it true for the http Keycloak; NEVER enable it in production.
	AuthAllowInsecureJWKS bool `env:"AUTH_ALLOW_INSECURE_JWKS" envDefault:"false"`
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
	// permit frees as soon as any stream ends. Default 4096 is a safe bulkhead
	// for a typical replica; set 0 to explicitly opt OUT of the cap (unlimited).
	SSEMaxStreams int `env:"GATEWAY_SSE_MAX_STREAMS" envDefault:"4096"`
	// EmbeddedProjection controls whether this gateway process runs the
	// read-model projection consumer (default true — single-binary demo
	// topology). Set false when the projection runs as its own deployment
	// (examples/gateway/cmd/projection): the gateway then serves HTTP only
	// and the standalone binary owns the consumer group. Both modes share the
	// same consumer group ("gateway-projection") and inbox table, so a
	// rolling migration between modes is safe.
	EmbeddedProjection bool `env:"GATEWAY_EMBEDDED_PROJECTION" envDefault:"true"`
	// ProjectionBatch runs the read-model projection in batch-apply mode: each
	// partition's poll records are applied in ONE transaction (one tx per
	// partition-batch-per-poll) instead of one tx per event — fewer commits,
	// higher projection throughput. Default true. The per-event path stays
	// available via GATEWAY_PROJECTION_BATCH=false. Both paths share the same
	// projection handlers, inbox dedup, offset ordering, post-commit cache-bust
	// + SSE notify hooks, and the WithRetry/DLT failure contract (a batch-tx
	// error falls back to the proven per-record path). Applies to both the
	// embedded projection and the standalone cmd/projection binary.
	ProjectionBatch bool `env:"GATEWAY_PROJECTION_BATCH" envDefault:"true"`
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
	// AttachmentContentTypes is the upload Content-Type allowlist for order
	// attachments. An upload whose media type is not listed is rejected with
	// 415 GATEWAY_ATTACHMENT_TYPE_REJECTED — defence-in-depth against stored
	// XSS (renderable types such as text/html and image/svg+xml are absent by
	// default; Download additionally forces an attachment disposition).
	// Empty falls back to attachments.DefaultAllowedContentTypes.
	AttachmentContentTypes []string `env:"GATEWAY_ATTACHMENT_CONTENT_TYPES" envSeparator:"," envDefault:"application/pdf,image/png,image/jpeg,image/gif,text/plain,application/octet-stream"`
	// RatelimitFailClosed makes the Redis-backed rate limiters DENY requests
	// when Redis is unavailable instead of failing open (the default). Trade-off:
	// fail-open preserves edge availability during a Redis outage but can admit
	// bursts beyond the configured rate; fail-closed enforces strictly at the
	// cost of denying traffic while Redis is down. The production preflight
	// REQUIRES this true when RATELIMIT_REDIS=true (see Validate).
	RatelimitFailClosed bool `env:"RATELIMIT_FAIL_CLOSED" envDefault:"false"`
	// AllowCleartextTransport is the explicit escape hatch for the
	// production cleartext-credential preflight (checkInsecureTransport). When
	// false (default) production REFUSES a Kafka SASL mechanism without
	// KAFKA_TLS_ENABLED=true, and a Redis password without REDIS_TLS_ENABLED=true
	// — either would send credentials in the clear. Set true ONLY when the
	// broker/Redis traffic rides a trusted private network (mTLS sidecar, VPC
	// peering, service mesh) where the operator accepts plaintext on the wire.
	AllowCleartextTransport bool `env:"APP_ALLOW_CLEARTEXT_TRANSPORT" envDefault:"false"`
}

// defaultDevPGDSN is the shipped development PG_DSN (see pg.Config). Booting
// production against it means the operator never set PG_DSN — a clear
// misconfiguration that the preflight rejects.
const defaultDevPGDSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // G101: this is the documented dev default mirrored from pg.Config — comparing against it is the feature

// Validate runs the production-safety preflight (W1.2). config.Load invokes it
// after env parsing (config.Validator hook). The checks RUN ONLY when
// APP_ENV=production — development and test keep the convenient-but-insecure
// shipped defaults — and a failed boot reports EVERY insecure value at once so
// the operator fixes them in a single pass.
//
// Each check fails closed on a value that would silently weaken the service's
// trust model in production: disabled auth, an unencrypted or defaulted
// database connection, plaintext object storage against a remote endpoint, or
// a wildcard CORS origin.
func (c Config) Validate() error {
	return config.RequireProductionSafety(
		c.checkAuthEnabled,
		c.checkJWKSSecure,
		c.checkPGSecure,
		c.checkS3Secure,
		c.checkCORSNotWildcard,
		c.checkRatelimitFailClosed,
		c.checkInsecureTransport,
	)
}

// checkJWKSSecure forbids an insecure JWKS configuration in production. Keys
// fetched over plaintext http can be swapped by a man-in-the-middle who could
// then mint tokens this gateway would trust — a full auth bypass. Two ways that
// surfaces, both rejected:
//   - AUTH_ALLOW_INSECURE_JWKS=true: the dev escape hatch must never be on in
//     production (it disables the verifier's own https guard).
//   - auth enabled with a non-https GATEWAY_JWKS_URL: even without the escape
//     hatch, a configured http URL is rejected here so the misconfiguration
//     fails the preflight loudly rather than only at verifier construction.
//
// An empty JWKS URL is not flagged here: that is a separate "auth enabled but
// no IdP configured" misconfiguration the verifier builder reports.
func (c Config) checkJWKSSecure() error {
	if c.AuthAllowInsecureJWKS {
		return errors.New("AUTH_ALLOW_INSECURE_JWKS=true is forbidden in production: an http JWKS can be MITM-swapped to forge tokens (auth bypass)")
	}
	if c.AuthDisabled || c.JWKSUrl == "" {
		return nil
	}
	if u, err := url.Parse(c.JWKSUrl); err != nil || u.Scheme != "https" {
		return fmt.Errorf("GATEWAY_JWKS_URL %q must use https in production: an http JWKS can be MITM-swapped to forge tokens", c.JWKSUrl)
	}
	return nil
}

// checkInsecureTransport forbids sending credentials in cleartext in production
// (security review #6). Kafka SASL and a Redis password both authenticate over
// the wire; without TLS the secret crosses the network in the clear. Each is
// allowed only when its transport is TLS-wrapped, or when the operator has
// explicitly accepted a trusted private network via
// APP_ALLOW_CLEARTEXT_TRANSPORT=true.
func (c Config) checkInsecureTransport() error {
	if c.AllowCleartextTransport {
		return nil
	}
	var errs []error
	if c.Kafka.SASLMechanism != "" && !c.Kafka.TLSEnabled {
		errs = append(errs, errors.New("KAFKA_SASL_MECHANISM is set with KAFKA_TLS_ENABLED=false in production: the SASL password would cross the network in cleartext (set KAFKA_TLS_ENABLED=true, or APP_ALLOW_CLEARTEXT_TRANSPORT=true for a trusted network)"))
	}
	if c.Cache.Password.Reveal() != "" && !c.Cache.TLSEnabled {
		errs = append(errs, errors.New("REDIS_PASSWORD is set with REDIS_TLS_ENABLED=false in production: the Redis AUTH password would cross the network in cleartext (set REDIS_TLS_ENABLED=true, or APP_ALLOW_CLEARTEXT_TRANSPORT=true for a trusted network)"))
	}
	return errors.Join(errs...)
}

func (c Config) checkAuthEnabled() error {
	if c.AuthDisabled {
		return errors.New("GATEWAY_AUTH_DISABLED=true is forbidden in production: the service would accept unauthenticated requests")
	}
	return nil
}

func (c Config) checkPGSecure() error {
	dsn := c.PG.DSN.Reveal()
	// The defaulted dev DSN is the more specific diagnosis (operator never set
	// PG_DSN at all) and also contains sslmode=disable — report it first.
	if dsn == defaultDevPGDSN {
		return errors.New("PG_DSN is the default development PG_DSN (postgres:postgres@localhost) — set a real production DSN")
	}
	if strings.Contains(dsn, "sslmode=disable") {
		return errors.New("PG_DSN must not contain sslmode=disable in production: the database connection would be unencrypted")
	}
	return nil
}

func (c Config) checkS3Secure() error {
	// Plaintext object storage is fine against a loopback sidecar but not a
	// remote endpoint, where credentials and payloads would cross the network
	// in the clear.
	if c.S3.UseSSL || isLoopbackEndpoint(c.S3.Endpoint) {
		return nil
	}
	return fmt.Errorf("S3_USE_SSL=false against a non-localhost endpoint (%q) is forbidden in production: object-store traffic would be unencrypted", c.S3.Endpoint)
}

func (c Config) checkCORSNotWildcard() error {
	if slices.Contains(c.CORSOrigins, "*") {
		return errors.New(`GATEWAY_CORS_ORIGINS must not be "*" in production: set explicit allowed origins`)
	}
	return nil
}

// checkRatelimitFailClosed forbids a fail-open distributed rate limiter in
// production. With RATELIMIT_REDIS=true the limiter is the edge's defence
// against floods; if it fails OPEN, a Redis outage silently lifts the limit and
// lets unbounded traffic through. Production must fail CLOSED so an outage
// sheds load instead of removing the guard. The check is scoped to
// RATELIMIT_REDIS=true: the in-memory limiter has no external dependency to
// fail open against.
func (c Config) checkRatelimitFailClosed() error {
	if c.RatelimitRedis && !c.RatelimitFailClosed {
		return errors.New("RATELIMIT_FAIL_CLOSED must be true in production when RATELIMIT_REDIS=true: a fail-open limiter lets a Redis outage silently remove the rate limit")
	}
	return nil
}

// isLoopbackEndpoint reports whether an S3 endpoint (host:port, optionally
// scheme-prefixed) targets the local machine, where plaintext is acceptable.
func isLoopbackEndpoint(endpoint string) bool {
	e := endpoint
	if i := strings.Index(e, "://"); i >= 0 {
		e = e[i+3:]
	}
	host := e
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	host = strings.TrimSpace(host)
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
