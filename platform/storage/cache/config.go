package cache

import "time"

// Config holds tunable parameters for the two-tier cache.
// Tags are compatible with github.com/caarlos0/env/v11.
type Config struct {
	// RedisAddrs is a comma-separated list of Redis addresses.
	RedisAddrs []string `env:"REDIS_ADDRS" envSeparator:"," envDefault:"localhost:6379"`
	// L1Capacity is the maximum number of entries held in the in-process cache.
	L1Capacity int `env:"CACHE_L1_CAPACITY" envDefault:"10000"`
	// DefaultTTL is the TTL used when the caller does not supply one (ttl <= 0).
	DefaultTTL time.Duration `env:"CACHE_DEFAULT_TTL" envDefault:"5m"`
	// TTLJitter is the fractional jitter applied to every TTL, e.g. 0.1 = ±10%.
	TTLJitter float64 `env:"CACHE_TTL_JITTER" envDefault:"0.1"`
}
