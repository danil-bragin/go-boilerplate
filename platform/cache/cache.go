// Package cache provides a two-tier cache: L1 in-process (otter v2) and L2
// distributed (rueidis/Redis). Concurrent misses for the same key are
// collapsed via singleflight. TTLs are jittered to spread expiry load.
package cache

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/redis/rueidis"
	"golang.org/x/sync/singleflight"

	"go-boilerplate/platform/cqrs"
)

// Compile-time assertion that *Cache satisfies cqrs.Cache.
var _ cqrs.Cache = (*Cache)(nil)

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

// Cache is a two-tier cache: L1 (in-process otter) + L2 (Redis via rueidis).
// It implements cqrs.Cache and adds GetOrLoad with singleflight stampede protection.
type Cache struct {
	l1  *otter.Cache[string, []byte]
	l2  rueidis.Client
	sf  singleflight.Group
	cfg Config
}

// New constructs a Cache from cfg.
// The caller is responsible for calling Close when done.
func New(cfg Config) (*Cache, error) {
	// Build L1 otter cache with per-entry TTL support.
	// ExpiryWritingFunc allows SetExpiresAfter to override per entry.
	l1, err := otter.New[string, []byte](&otter.Options[string, []byte]{
		MaximumSize: cfg.L1Capacity,
		// A default expiry so entries do not live forever when SetExpiresAfter
		// is not called. Individual entries override this via SetExpiresAfter.
		ExpiryCalculator: otter.ExpiryWriting[string, []byte](cfg.DefaultTTL),
	})
	if err != nil {
		return nil, err
	}

	// Build L2 rueidis client.
	l2, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: cfg.RedisAddrs,
	})
	if err != nil {
		l1.InvalidateAll()
		return nil, err
	}

	return &Cache{l1: l1, l2: l2, cfg: cfg}, nil
}

// JitteredTTL returns ttl adjusted by a random ±TTLJitter fraction.
// If ttl <= 0 the cache DefaultTTL is used as the base.
// This is exported so tests can verify the distribution directly.
func (c *Cache) JitteredTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = c.cfg.DefaultTTL
	}
	jitter := c.cfg.TTLJitter
	if jitter <= 0 {
		return ttl
	}
	// delta ∈ [-jitter*ttl, +jitter*ttl]
	delta := time.Duration(float64(ttl) * jitter * (2*rand.Float64() - 1))
	result := ttl + delta
	if result <= 0 {
		result = ttl
	}
	return result
}

// Get looks up key in L1 first; on a miss it falls back to L2.
// A L2 hit populates L1 with a jittered DefaultTTL.
// Returns (value, true) on hit, (nil, false) on miss.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool) {
	// L1 fast path.
	if v, ok := c.l1.GetIfPresent(key); ok {
		return v, true
	}

	// L2 path — use client-side caching with a short client-side TTL to
	// benefit from rueidis's built-in invalidation protocol.
	res := c.l2.DoCache(ctx, c.l2.B().Get().Key(key).Cache(), c.cfg.DefaultTTL)
	b, err := res.AsBytes()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, false
		}
		// Network / protocol error — treat as miss.
		return nil, false
	}

	// Populate L1.
	jttl := c.JitteredTTL(c.cfg.DefaultTTL)
	c.l1.Set(key, b)
	c.l1.SetExpiresAfter(key, jttl)
	return b, true
}

// Set writes val into both L2 (Redis) and L1 with a jittered ttl.
// L2 errors are silently ignored so cache writes never fail the caller.
// TODO(obs): surface L2 write errors via metrics/logging once an obs package exists.
func (c *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) {
	jttl := c.JitteredTTL(ttl)

	// Write to L2 first so that other nodes can benefit immediately.
	// rueidis SET key val EX <seconds>
	_ = c.l2.Do(ctx, c.l2.B().Set().Key(key).Value(rueidis.BinaryString(val)).Ex(jttl).Build()).Error()
	// TODO(obs): log L2 error.

	// Write to L1.
	c.l1.Set(key, val)
	c.l1.SetExpiresAfter(key, jttl)
}

// GetOrLoad returns the cached value for key, loading it with loader on a
// miss. Concurrent misses for the same key are collapsed via singleflight so
// loader is called at most once per key across all goroutines.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok := c.Get(ctx, key); ok {
		return v, nil
	}

	v, err, _ := c.sf.Do(key, func() (any, error) {
		// Double-check after acquiring the singleflight token.
		if v, ok := c.Get(ctx, key); ok {
			return v, nil
		}
		val, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		c.Set(ctx, key, val, ttl)
		return val, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil //nolint:forcetypeassert
}

// HealthCheck pings Redis and returns any error.
func (c *Cache) HealthCheck(ctx context.Context) error {
	return c.l2.Do(ctx, c.l2.B().Ping().Build()).Error()
}

// Close releases the rueidis connection pool.
func (c *Cache) Close(_ context.Context) error {
	c.l2.Close()
	return nil
}
