package cache_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"golang.org/x/sync/errgroup"

	"go-boilerplate/platform/cache"
)

// newCache starts a Redis testcontainer and returns a configured *Cache.
// The container and cache are both cleaned up via t.Cleanup.
func newCache(t *testing.T) *cache.Cache {
	t.Helper()
	ctx := context.Background()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rc.Terminate(context.Background())
	})

	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	// ConnectionString returns "redis://host:port" — strip the scheme.
	// rueidis wants "host:port".
	addr = stripRedisScheme(addr)

	cfg := cache.Config{
		RedisAddrs: []string{addr},
		L1Capacity: 1000,
		DefaultTTL: time.Minute,
		TTLJitter:  0.1,
	}
	c, err := cache.New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.Close(context.Background())
	})
	return c
}

// stripRedisScheme removes "redis://" prefix if present.
func stripRedisScheme(s string) string {
	const prefix = "redis://"
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

// TestCache_SetGetRoundTrip verifies basic write/read semantics.
func TestCache_SetGetRoundTrip(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	// Unknown key → miss.
	_, ok := c.Get(ctx, "nonexistent")
	assert.False(t, ok, "expected miss for unknown key")

	// Write then read back.
	c.Set(ctx, "hello", []byte("world"), time.Minute)

	v, ok := c.Get(ctx, "hello")
	require.True(t, ok, "expected hit after Set")
	assert.Equal(t, []byte("world"), v)
}

// TestCache_L1ServesAfterL2 verifies that after a Set, a second Get returns
// the correct value (either from L1 or L2).
func TestCache_L1ServesAfterL2(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	c.Set(ctx, "key1", []byte("value1"), time.Minute)

	// First Get: may come from L1 (if Set populated it) or L2.
	v, ok := c.Get(ctx, "key1")
	require.True(t, ok)
	assert.Equal(t, []byte("value1"), v)

	// Second Get: must still return correct value.
	v2, ok2 := c.Get(ctx, "key1")
	require.True(t, ok2)
	assert.Equal(t, []byte("value1"), v2)
}

// TestCache_GetOrLoadCollapsesConcurrentMisses asserts that 20 concurrent
// callers all receive the same result and that the loader is called exactly
// once (singleflight collapses the misses).
func TestCache_GetOrLoadCollapsesConcurrentMisses(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	var calls atomic.Int64

	loader := func(_ context.Context) ([]byte, error) {
		// Simulate an expensive fetch.
		time.Sleep(50 * time.Millisecond)
		calls.Add(1)
		return []byte("X"), nil
	}

	const goroutines = 20
	eg, egCtx := errgroup.WithContext(ctx)
	results := make([][]byte, goroutines)

	for i := range goroutines {
		i := i
		eg.Go(func() error {
			v, err := c.GetOrLoad(egCtx, "hot", time.Minute, loader)
			if err != nil {
				return err
			}
			results[i] = v
			return nil
		})
	}

	require.NoError(t, eg.Wait())

	// All goroutines must receive the correct value.
	for i, v := range results {
		assert.Equal(t, []byte("X"), v, "goroutine %d got unexpected value", i)
	}

	// Singleflight must have collapsed all misses to a single loader call.
	assert.Equal(t, int64(1), calls.Load(), "loader should be called exactly once")
}

// TestCache_TTLJitterWithinBounds verifies that JitteredTTL always falls
// within the expected ±jitter% band.
func TestCache_TTLJitterWithinBounds(t *testing.T) {
	c, err := cache.New(cache.Config{
		// Use a fake address; we don't need Redis for this test.
		// However New dials Redis — use a real container to avoid errors.
		// (We rely on newCache in other tests; here we only test JitteredTTL
		// which is pure math, so we use a cache from newCache.)
		RedisAddrs: []string{"127.0.0.1:6399"}, // will likely fail
		L1Capacity: 10,
		DefaultTTL: time.Minute,
		TTLJitter:  0.1,
	})
	// If Redis is not available we still want to test the math.
	// New may fail, but we can create a minimal cache with a real container.
	_ = c
	_ = err

	// Use a fresh cache from the container helper.
	cc := newCache(t)

	base := 100 * time.Second
	jitter := 0.1
	lo := time.Duration(float64(base) * (1 - jitter))
	hi := time.Duration(float64(base) * (1 + jitter))

	for range 10_000 {
		got := cc.JitteredTTL(base)
		assert.GreaterOrEqual(t, got, lo, "jittered TTL below lower bound")
		assert.LessOrEqual(t, got, hi, "jittered TTL above upper bound")
	}
}

// TestCache_HealthCheck verifies the HealthCheck does not return an error
// when Redis is up.
func TestCache_HealthCheck(t *testing.T) {
	c := newCache(t)
	err := c.HealthCheck(context.Background())
	assert.NoError(t, err)
}
