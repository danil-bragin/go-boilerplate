package gateway

import (
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigDefaults pins the env defaults that the round-8 hardening changed:
// the SSE bulkhead now defaults to a positive cap (4096) — a safe per-replica
// guard — while 0 stays the explicit opt-out for unlimited. The attachment
// content-type allowlist defaults to a conservative doc/image set.
func TestConfigDefaults(t *testing.T) {
	// config.Load runs Validate(); outside production it is a no-op, so the
	// shipped insecure-but-convenient defaults load cleanly.
	t.Setenv("APP_ENV", "development")

	cfg, err := config.Load[Config]()
	require.NoError(t, err)

	assert.Equal(t, 4096, cfg.SSEMaxStreams, "SSE bulkhead must default to a positive cap")
	assert.False(t, cfg.RatelimitFailClosed, "fail-open is the documented default")
	assert.Contains(t, cfg.AttachmentContentTypes, "application/pdf")
	assert.Contains(t, cfg.AttachmentContentTypes, "image/png")
	assert.NotContains(t, cfg.AttachmentContentTypes, "text/html", "renderable types must not be in the default allowlist")
}

// safeProdConfig returns a Config that passes every production preflight check,
// so each table case can mutate exactly one field to the insecure value and
// prove THAT check (and only that check) fires.
func safeProdConfig() Config {
	var c Config
	c.AuthDisabled = false
	// PG is the embedded servicekit.Config.PG.
	c.PG = pg.Config{DSN: "postgres://app:s3cret@db.internal:5432/app?sslmode=require"}
	c.S3 = blob.Config{Endpoint: "s3.internal:443", UseSSL: true}
	c.CORSOrigins = []string{"https://app.example.com"}
	c.JWKSUrl = "https://idp.internal/realms/app/protocol/openid-connect/certs"
	return c
}

func TestConfigValidate_ProductionRejectsInsecure(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantSub string
	}{
		{
			name:    "auth disabled",
			mutate:  func(c *Config) { c.AuthDisabled = true },
			wantSub: "GATEWAY_AUTH_DISABLED",
		},
		{
			name:    "pg sslmode=disable",
			mutate:  func(c *Config) { c.PG.DSN = "postgres://app:pw@db.internal:5432/app?sslmode=disable" },
			wantSub: "sslmode=disable",
		},
		{
			name: "defaulted dev pg dsn",
			mutate: func(c *Config) {
				c.PG.DSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
			},
			wantSub: "default development PG_DSN",
		},
		{
			name:    "plaintext s3 non-localhost",
			mutate:  func(c *Config) { c.S3 = blob.Config{Endpoint: "minio.internal:9000", UseSSL: false} },
			wantSub: "S3_USE_SSL",
		},
		{
			name:    "cors wildcard",
			mutate:  func(c *Config) { c.CORSOrigins = []string{"*"} },
			wantSub: "GATEWAY_CORS_ORIGINS",
		},
		{
			name: "ratelimit fail-open with redis",
			mutate: func(c *Config) {
				c.RatelimitRedis = true
				c.RatelimitFailClosed = false
			},
			wantSub: "RATELIMIT_FAIL_CLOSED",
		},
		{
			name:    "insecure jwks escape hatch",
			mutate:  func(c *Config) { c.AuthAllowInsecureJWKS = true },
			wantSub: "AUTH_ALLOW_INSECURE_JWKS",
		},
		{
			name:    "non-https jwks url with auth enabled",
			mutate:  func(c *Config) { c.JWKSUrl = "http://idp.internal/realms/app/certs" },
			wantSub: "GATEWAY_JWKS_URL",
		},
		{
			name: "kafka SASL without TLS",
			mutate: func(c *Config) {
				c.Kafka.SASLMechanism = "PLAIN"
				c.Kafka.TLSEnabled = false
			},
			wantSub: "KAFKA_SASL_MECHANISM",
		},
		{
			name: "redis password without TLS",
			mutate: func(c *Config) {
				c.Cache.Password = "s3cret"
				c.Cache.TLSEnabled = false
			},
			wantSub: "REDIS_PASSWORD",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := safeProdConfig()
			tc.mutate(&c)
			err := c.Validate()
			require.Error(t, err, "production must reject %s", tc.name)
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}

// TestConfigValidate_PlaintextS3AllowedForLocalhost: plaintext S3 against a
// localhost endpoint is fine even in production (a sidecar/loopback MinIO),
// so the check must NOT fire for localhost/127.0.0.1.
func TestConfigValidate_PlaintextS3AllowedForLocalhost(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	for _, ep := range []string{"localhost:9000", "127.0.0.1:9000", "http://localhost:9000"} {
		c := safeProdConfig()
		c.S3 = blob.Config{Endpoint: ep, UseSSL: false}
		assert.NoError(t, c.Validate(), "plaintext S3 against %s must be allowed", ep)
	}
}

// TestConfigValidate_DevelopmentAllowsInsecure: outside production every
// insecure default is permitted (local dev convenience).
func TestConfigValidate_DevelopmentAllowsInsecure(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	c := safeProdConfig()
	c.AuthDisabled = true
	c.PG.DSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	c.S3 = blob.Config{Endpoint: "minio.internal:9000", UseSSL: false}
	c.CORSOrigins = []string{"*"}
	assert.NoError(t, c.Validate(), "development must allow insecure defaults")
}

// TestConfigValidate_ProductionAllSafe: a fully-hardened production config
// passes.
func TestConfigValidate_ProductionAllSafe(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	assert.NoError(t, safeProdConfig().Validate())
}

// TestConfigValidate_JWKSSecure covers the auth-bypass preflight (#1): an empty
// JWKS URL is not flagged (separate misconfiguration), an https URL passes, and
// the cleartext escape hatch / http URL are rejected by the table test above.
func TestConfigValidate_JWKSSecure(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	t.Run("empty jwks url not flagged here", func(t *testing.T) {
		c := safeProdConfig()
		c.JWKSUrl = ""
		assert.NoError(t, c.Validate(), "empty JWKS URL is a separate concern, not the https guard")
	})
	t.Run("https jwks url passes", func(t *testing.T) {
		c := safeProdConfig()
		c.JWKSUrl = "https://idp.example.com/certs"
		assert.NoError(t, c.Validate())
	})
	t.Run("auth disabled skips jwks url check", func(t *testing.T) {
		c := safeProdConfig()
		c.AuthDisabled = true // this trips checkAuthEnabled, but not the JWKS-url check
		c.JWKSUrl = "http://idp.example.com/certs"
		err := c.Validate()
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "GATEWAY_JWKS_URL", "JWKS-url check is skipped when auth is disabled")
	})
}

// TestConfigValidate_InsecureTransport covers the cleartext-credential preflight
// (#6): SASL/Redis-password over TLS passes, and the explicit trusted-network
// escape hatch (APP_ALLOW_CLEARTEXT_TRANSPORT) allows plaintext.
func TestConfigValidate_InsecureTransport(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	t.Run("kafka SASL with TLS passes", func(t *testing.T) {
		c := safeProdConfig()
		c.Kafka.SASLMechanism = "SCRAM-SHA-256"
		c.Kafka.TLSEnabled = true
		assert.NoError(t, c.Validate())
	})
	t.Run("redis password with TLS passes", func(t *testing.T) {
		c := safeProdConfig()
		c.Cache.Password = "s3cret"
		c.Cache.TLSEnabled = true
		assert.NoError(t, c.Validate())
	})
	t.Run("escape hatch allows cleartext", func(t *testing.T) {
		c := safeProdConfig()
		c.AllowCleartextTransport = true
		c.Kafka.SASLMechanism = "PLAIN"
		c.Kafka.TLSEnabled = false
		c.Cache.Password = "s3cret"
		c.Cache.TLSEnabled = false
		assert.NoError(t, c.Validate(), "explicit trusted-network opt-in permits cleartext")
	})
}

// TestConfigValidate_RatelimitFailClosed: the fail-open production guard is
// scoped to RATELIMIT_REDIS=true. A fail-open in-memory limiter (no Redis) is
// fine — there is no external dependency to fail open against — and a
// Redis-backed limiter that fails CLOSED passes.
func TestConfigValidate_RatelimitFailClosed(t *testing.T) {
	t.Setenv("APP_ENV", "production")

	t.Run("redis fail-closed passes", func(t *testing.T) {
		c := safeProdConfig()
		c.RatelimitRedis = true
		c.RatelimitFailClosed = true
		assert.NoError(t, c.Validate())
	})

	t.Run("in-memory fail-open allowed", func(t *testing.T) {
		c := safeProdConfig()
		c.RatelimitRedis = false
		c.RatelimitFailClosed = false
		assert.NoError(t, c.Validate(), "in-memory limiter has no backend to fail open against")
	})
}
