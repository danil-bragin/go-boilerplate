// Package cache provides a two-tier cache: L1 in-process (otter v2) and L2
// distributed (rueidis/Redis). Concurrent misses for the same key are
// collapsed via singleflight. TTLs are jittered to spread expiry load.
//
// # Key convention
//
// Cache keys follow "<svc>:v<N>:<entity>:<id>" (e.g. "gw:v1:order:1234").
// The version segment is bumped whenever the cached value's shape changes —
// old entries become unreachable and simply expire, instead of unmarshalling
// stale bytes into the new shape. See docs/conventions.md for the full rule.
package cache

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/observability/log"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/maypok86/otter/v2"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

// Compile-time assertion that *Cache satisfies cqrs.Cache.
var _ cqrs.Cache = (*Cache)(nil)

// Cache is a two-tier cache: L1 (in-process otter) + L2 (Redis via rueidis).
// It implements cqrs.Cache and adds GetOrLoad with singleflight stampede protection.
//
// Coherence notes:
//
//   - Cross-instance L1 coherence is maintained via a Redis pub/sub broadcast:
//     every Set and Delete publishes the key on "cache:inv:<prefix>" and other
//     instances drop the key from their L1 (L2 stays authoritative). Delivery
//     is best-effort — a dropped broadcast (Redis hiccup, resubscribe window)
//     degrades to TTL-bounded staleness, so keep TTLs modest as the floor.
//
//   - An L2 (Redis) error on read is treated as a cache miss (availability over
//     strictness). Such errors are surfaced via the "cache.l2.errors" counter
//     ({op=get|set|del}) and a WARN log (see l2Error) — never to the caller.
type Cache struct {
	l1  *otter.Cache[string, []byte]
	l2  rueidis.Client
	sf  singleflight.Group
	cfg Config

	// L2 circuit breaker (see breaker.go): a Redis outage degrades to
	// L1-only instead of a per-request connection wait.
	l2cb      circuitbreaker.CircuitBreaker[any]
	l2cbGauge metric.Int64Gauge

	// L2 error counter ("cache.l2.errors", {op=get|set|del}); nil-degrades.
	l2errs metric.Int64Counter

	// Pub/sub invalidation broadcast state (see invalidation.go).
	instanceID string
	subCancel  context.CancelFunc
	subWG      sync.WaitGroup

	// receive is the blocking subscription primitive used by subscribeLoop.
	// A struct field so tests can substitute a stub; when nil it defaults to
	// rueidis Receive on the invalidation channel.
	receive func(ctx context.Context, fn func(msg rueidis.PubSubMessage)) error
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

	// Build L2 rueidis client (password + TLS via the shared option seam).
	l2, err := rueidis.NewClient(BuildRueidisOption(cfg))
	if err != nil {
		l1.InvalidateAll()
		return nil, err
	}

	c := &Cache{l1: l1, l2: l2, cfg: cfg}
	c.l2cb, c.l2cbGauge = newL2Breaker()
	c.l2errs = newL2ErrorCounter()
	c.startInvalidationSubscriber()
	return c, nil
}

// l2Error surfaces a (previously silent) L2 failure: it increments the
// "cache.l2.errors" counter for op and emits a WARN on the ctx logger.
//
// Failure attribution mirrors l2Done: an op that failed because the CALLER's
// ctx ended says nothing about Redis health, so it is neither counted nor
// logged — a burst of cancelled requests must not look like an L2 outage.
func (c *Cache) l2Error(ctx context.Context, op, key string, err error) {
	if ctx.Err() != nil {
		return
	}
	if c.l2errs != nil {
		c.l2errs.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op)))
	}
	log.From(ctx).Warn("cache: L2 error", "op", op, "key", key, "error", err)
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
	delta := time.Duration(float64(ttl) * jitter * (2*rand.Float64() - 1)) //nolint:gosec // G404: math/rand is fine for cache TTL jitter, not a security context
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
	// Return a copy so the caller cannot mutate the shared L1 entry.
	if v, ok := c.l1.GetIfPresent(key); ok {
		out := make([]byte, len(v))
		copy(out, v)
		return out, true
	}

	// L2 path — skipped entirely while the breaker is open (L1-only mode).
	if !c.l2Allowed(ctx) {
		return nil, false
	}

	// Use client-side caching with a short client-side TTL to benefit from
	// rueidis's built-in invalidation protocol.
	opCtx, opCancel := c.l2ctx(ctx)
	res := c.l2.DoCache(opCtx, c.l2.B().Get().Key(key).Cache(), c.cfg.DefaultTTL)
	opCancel()
	b, err := res.AsBytes()
	c.l2Done(ctx, err)
	if err != nil {
		// Redis nil → genuine miss; network / protocol error → treat as
		// miss too (the breaker absorbs repeated failures) but make it
		// visible: counter + WARN instead of the historical silent ignore.
		if !rueidis.IsRedisNil(err) {
			c.l2Error(ctx, "get", key, err)
		}
		return nil, false
	}

	// Store a copy in L1 so the returned slice and the L1 entry don't alias.
	l1copy := make([]byte, len(b))
	copy(l1copy, b)
	jttl := c.JitteredTTL(c.cfg.DefaultTTL)
	c.l1.Set(key, l1copy)
	c.l1.SetExpiresAfter(key, jttl)
	// Return b directly — rueidis AsBytes already allocated fresh bytes.
	return b, true
}

// Set writes val into both L2 (Redis) and L1 with a jittered ttl.
// L2 errors never fail the caller; they surface via the "cache.l2.errors"
// counter and a WARN log (see l2Error).
func (c *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) {
	jttl := c.JitteredTTL(ttl)

	// Write to L2 first so that other nodes can benefit immediately
	// (skipped while the breaker is open — L1-only mode).
	// rueidis SET key val EX <seconds>
	if c.l2Allowed(ctx) {
		opCtx, opCancel := c.l2ctx(ctx)
		err := c.l2.Do(opCtx, c.l2.B().Set().Key(key).Value(rueidis.BinaryString(val)).Ex(jttl).Build()).Error()
		opCancel()
		c.l2Done(ctx, err)
		if err != nil {
			c.l2Error(ctx, "set", key, err)
		}

		// Broadcast so other instances drop their (now stale) L1 entry and
		// re-read the new value from L2. The receiver skips this instance
		// by id.
		c.publishInvalidation(ctx, key)
	}

	// Write a copy to L1 so that a caller mutating val after Set
	// does not corrupt the shared in-process entry.
	l1copy := make([]byte, len(val))
	copy(l1copy, val)
	c.l1.Set(key, l1copy)
	c.l1.SetExpiresAfter(key, jttl)
}

// Delete removes key from L1 and L2 and broadcasts the eviction so every
// other instance drops its L1 entry too. The L2 DEL error is returned (the
// caller may want to know the authoritative tier still holds the key); the
// broadcast itself is best-effort. While the L2 breaker is open the local L1
// eviction still happens but ErrL2Unavailable is returned.
func (c *Cache) Delete(ctx context.Context, key string) error {
	c.l1.Invalidate(key)

	if !c.l2Allowed(ctx) {
		return ErrL2Unavailable
	}
	opCtx, opCancel := c.l2ctx(ctx)
	err := c.l2.Do(opCtx, c.l2.B().Del().Key(key).Build()).Error()
	opCancel()
	c.l2Done(ctx, err)
	if err != nil {
		c.l2Error(ctx, "del", key, err)
	}

	c.publishInvalidation(ctx, key)
	return err
}

// GetOrLoad returns the cached value for key, loading it with loader on a
// miss. Concurrent misses for the same key are collapsed via singleflight so
// loader is called at most once per key across all goroutines.
//
// The loader runs on a context DETACHED from the first caller
// (context.WithoutCancel) bounded by Config.LoaderTimeout: collapsed waiters
// share one load, so the leader's cancellation must not fail everyone else.
//
// Each WAITER, however, honours its own context: when the caller's ctx ends
// before the shared load completes, GetOrLoad returns ctx.Err() immediately
// (the caller's Deadline budget is respected) while the load keeps running in
// the background for the remaining waiters and to warm the cache.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok := c.Get(ctx, key); ok {
		return v, nil
	}

	ch := c.sf.DoChan(key, func() (any, error) {
		// Detach from the leader's cancellation up front: this closure may
		// outlive the leader and serve other waiters.
		base := context.WithoutCancel(ctx)

		// Double-check after acquiring the singleflight token.
		if v, ok := c.Get(base, key); ok {
			return v, nil
		}
		// Bound with our own timeout so a hung loader cannot wedge the
		// singleflight slot forever.
		timeout := c.cfg.LoaderTimeout
		if timeout <= 0 {
			timeout = defaultLoaderTimeout
		}
		lctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()

		val, err := loader(lctx)
		if err != nil {
			return nil, err
		}
		c.Set(lctx, key, val, ttl)
		return val, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]byte), nil //nolint:forcetypeassert
	}
}

// HealthCheck pings Redis and returns any error.
func (c *Cache) HealthCheck(ctx context.Context) error {
	return c.l2.Do(ctx, c.l2.B().Ping().Build()).Error()
}

// Close stops the invalidation subscriber, stops otter's background
// goroutines and releases the rueidis connection pool.
func (c *Cache) Close(_ context.Context) error {
	c.stopInvalidationSubscriber()
	c.l1.StopAllGoroutines()
	c.l2.Close()
	return nil
}
