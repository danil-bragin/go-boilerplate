package cache

import (
	"crypto/tls"
	"time"

	"go-boilerplate/platform/config"

	"github.com/redis/rueidis"
)

// defaultLoaderTimeout bounds GetOrLoad loaders when Config.LoaderTimeout is
// unset (the loader runs detached from the caller's cancellation).
const defaultLoaderTimeout = 10 * time.Second

// defaultL2OpTimeout bounds a single L2 (Redis) operation when
// Config.L2OpTimeout is unset. rueidis retries network errors until the ctx
// deadline, so an unbounded ctx would hang every op during an outage and the
// circuit breaker would never see a failure.
const defaultL2OpTimeout = time.Second

// Config holds tunable parameters for the two-tier cache.
// Tags are compatible with github.com/caarlos0/env/v11.
type Config struct {
	// RedisAddrs is a comma-separated list of Redis addresses.
	RedisAddrs []string `env:"REDIS_ADDRS" envSeparator:"," envDefault:"localhost:6379"`
	// Password authenticates to Redis (AUTH / requirepass). Empty = no auth
	// (back-compatible default). config.Secret keeps it out of log/JSON/YAML
	// dumps. Used by the cache client AND the gateway's ratelimit + SSE
	// rueidis clients via buildRueidisOption.
	Password config.Secret `env:"REDIS_PASSWORD" envDefault:""`
	// TLSEnabled wraps the Redis connection in TLS. Enable it whenever Redis
	// auth credentials or cached payloads traverse an untrusted network.
	TLSEnabled bool `env:"REDIS_TLS_ENABLED" envDefault:"false"`
	// L1Capacity is the maximum number of entries held in the in-process cache.
	L1Capacity int `env:"CACHE_L1_CAPACITY" envDefault:"10000"`
	// DefaultTTL is the TTL used when the caller does not supply one (ttl <= 0).
	DefaultTTL time.Duration `env:"CACHE_DEFAULT_TTL" envDefault:"5m"`
	// TTLJitter is the fractional jitter applied to every TTL, e.g. 0.1 = ±10%.
	TTLJitter float64 `env:"CACHE_TTL_JITTER" envDefault:"0.1"`
	// InvalidationPrefix names the pub/sub invalidation channel
	// ("cache:inv:<prefix>"). Instances sharing a prefix form one L1
	// coherence domain; give each service its own prefix.
	InvalidationPrefix string `env:"CACHE_INV_PREFIX" envDefault:"default"`
	// LoaderTimeout bounds a GetOrLoad loader call. The loader runs on a
	// context detached from the first caller, so this timeout is its only
	// cancellation source. <= 0 falls back to 10s.
	LoaderTimeout time.Duration `env:"CACHE_LOADER_TIMEOUT" envDefault:"10s"`
	// L2OpTimeout bounds every single L2 (Redis) operation. Failures surface
	// within this bound and feed the L2 circuit breaker. <= 0 falls back
	// to 1s.
	L2OpTimeout time.Duration `env:"CACHE_L2_OP_TIMEOUT" envDefault:"1s"`
}

// BuildRueidisOption assembles a rueidis.ClientOption from cfg's Redis address,
// password, and TLS settings. It is the single seam every rueidis client in
// the service shares (cache.New plus the gateway's ratelimit and SSE clients)
// so that auth and transport security stay consistent across all of them.
//
// An empty Password leaves the connection unauthenticated; TLSEnabled=false
// leaves it cleartext (both back-compatible defaults).
func BuildRueidisOption(cfg Config) rueidis.ClientOption {
	opt := rueidis.ClientOption{
		InitAddress: cfg.RedisAddrs,
		Password:    cfg.Password.Reveal(),
	}
	if cfg.TLSEnabled {
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return opt
}
