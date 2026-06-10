package cache_test

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/storage/cache"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
)

// newRedisContainer starts a Redis testcontainer and returns its address in
// "host:port" form. The container is cleaned up via t.Cleanup.
func newRedisContainer(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
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
	return stripRedisScheme(addr)
}

// newCacheAt builds a *Cache against the given Redis address, cleaned up via
// t.Cleanup.
func newCacheAt(t *testing.T, addr string) *cache.Cache {
	t.Helper()
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

// newCache starts a Redis testcontainer and returns a configured *Cache.
// The container and cache are both cleaned up via t.Cleanup.
func newCache(t *testing.T) *cache.Cache {
	t.Helper()
	return newCacheAt(t, newRedisContainer(t))
}

// freeLocalPort grabs a free TCP port from the kernel and releases it.
// Tiny reuse race is acceptable in tests.
func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert
	require.NoError(t, l.Close())
	return port
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

// TestCache_GetOrLoad_LoaderSurvivesCallerCancellation verifies that the
// loader runs on a context detached from the first caller: cancelling the
// leader's ctx mid-load neither cancels the loader nor fails the collapsed
// waiters.
func TestCache_GetOrLoad_LoaderSurvivesCallerCancellation(t *testing.T) {
	c := newCache(t)

	var (
		calls        atomic.Int64
		loaderCtxErr atomic.Value // error observed by the loader at completion
	)
	loaderStarted := make(chan struct{})

	loader := func(lctx context.Context) ([]byte, error) {
		calls.Add(1)
		close(loaderStarted)
		time.Sleep(200 * time.Millisecond)
		if err := lctx.Err(); err != nil {
			loaderCtxErr.Store(err)
			return nil, err
		}
		return []byte("X"), nil
	}

	// Leader with a cancellable ctx.
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = c.GetOrLoad(leaderCtx, "hot", time.Minute, loader)
	}()

	// Wait for the loader to be in flight, then cancel the leader.
	<-loaderStarted
	cancel()

	// Collapsed waiters with healthy ctx must still receive the value.
	const waiters = 5
	eg := errgroup.Group{}
	for range waiters {
		eg.Go(func() error {
			v, err := c.GetOrLoad(context.Background(), "hot", time.Minute, loader)
			if err != nil {
				return err
			}
			if string(v) != "X" {
				return fmt.Errorf("unexpected value %q", v)
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait(), "collapsed waiters must get the value despite leader cancellation")
	<-leaderDone

	assert.Equal(t, int64(1), calls.Load(), "loader must run exactly once")
	assert.Nil(t, loaderCtxErr.Load(), "loader ctx must not be cancelled by the leader's cancellation")
}

// TestCache_GetOrLoad_CallerDeadlineRespected verifies that a caller whose
// context deadline expires while the singleflight load is in flight gets
// DeadlineExceeded promptly (its Deadline budget is honored) while the load
// itself continues in the background for other waiters.
func TestCache_GetOrLoad_CallerDeadlineRespected(t *testing.T) {
	c := newCache(t)

	var calls atomic.Int64
	loader := func(_ context.Context) ([]byte, error) {
		calls.Add(1)
		time.Sleep(1 * time.Second)
		return []byte("X"), nil
	}

	// Caller with a 50ms deadline against a 1s loader.
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.GetOrLoad(shortCtx, "slow", time.Minute, loader)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded,
		"caller must get its own deadline error, not block for the full load")
	assert.Less(t, elapsed, 500*time.Millisecond,
		"caller must return around its 50ms deadline, not after the 1s load (took %v)", elapsed)

	// A second caller without a deadline still gets the value from the SAME
	// background load (no second loader invocation).
	v, err := c.GetOrLoad(context.Background(), "slow", time.Minute, loader)
	require.NoError(t, err)
	assert.Equal(t, []byte("X"), v)
	assert.Equal(t, int64(1), calls.Load(),
		"the load must have continued in the background; no second loader call")
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

// TestCache_ReturnedBytesAreCopy_NoAliasing verifies that:
//  1. Mutating the slice returned by Get does not corrupt the cached value for
//     subsequent readers (no shared L1 alias).
//  2. Mutating the slice passed to Set after the call does not corrupt the
//     cached value (Set copies before storing in L1).
//
// Run with -race to catch concurrent aliasing.
func TestCache_ReturnedBytesAreCopy_NoAliasing(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	// --- Part 1: mutating the returned slice must not corrupt the cache ---
	c.Set(ctx, "k", []byte("hello"), time.Minute)

	v, ok := c.Get(ctx, "k")
	require.True(t, ok)
	require.Equal(t, []byte("hello"), v)

	// Mutate the returned slice.
	v[0] = 'X'

	// A subsequent Get must still return the original value.
	v2, ok2 := c.Get(ctx, "k")
	require.True(t, ok2)
	assert.Equal(t, []byte("hello"), v2, "mutating the returned slice corrupted the cache (aliasing bug)")

	// --- Part 2: mutating the input slice after Set must not corrupt the cache ---
	original := []byte("world")
	c.Set(ctx, "k2", original, time.Minute)

	// Mutate the original buffer after Set.
	original[0] = 'Z'

	v3, ok3 := c.Get(ctx, "k2")
	require.True(t, ok3)
	assert.Equal(t, []byte("world"), v3, "mutating the Set input slice corrupted the cache (aliasing bug)")
}

// TestCache_DeleteEvictsLocally verifies that Delete removes the key from both
// tiers on the deleting instance itself.
func TestCache_DeleteEvictsLocally(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	c.Set(ctx, "k", []byte("v"), time.Minute)
	_, ok := c.Get(ctx, "k")
	require.True(t, ok)

	require.NoError(t, c.Delete(ctx, "k"))

	_, ok = c.Get(ctx, "k")
	assert.False(t, ok, "key must be gone after Delete")
}

// TestCache_CrossInstanceInvalidation_Delete verifies that a Delete on
// instance A evicts the warm L1 entry on instance B via the Redis pub/sub
// invalidation broadcast.
func TestCache_CrossInstanceInvalidation_Delete(t *testing.T) {
	addr := newRedisContainer(t)
	a := newCacheAt(t, addr)
	b := newCacheAt(t, addr)
	ctx := context.Background()

	a.Set(ctx, "k", []byte("v1"), time.Minute)

	// Warm B's L1 from L2.
	v, ok := b.Get(ctx, "k")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), v)

	require.NoError(t, a.Delete(ctx, "k"))

	// B's L1 must be evicted within 500ms; subsequent Get is a miss
	// (L2 was DELed and L1 dropped via broadcast).
	assert.Eventually(t, func() bool {
		_, ok := b.Get(ctx, "k")
		return !ok
	}, 500*time.Millisecond, 10*time.Millisecond, "B still serves stale L1 after A.Delete")
}

// TestCache_CrossInstanceInvalidation_SetBroadcast verifies that a Set on
// instance A invalidates the stale L1 entry on instance B, so B observes the
// new value within 500ms.
func TestCache_CrossInstanceInvalidation_SetBroadcast(t *testing.T) {
	addr := newRedisContainer(t)
	a := newCacheAt(t, addr)
	b := newCacheAt(t, addr)
	ctx := context.Background()

	a.Set(ctx, "k", []byte("v1"), time.Minute)

	// Warm B's L1 with v1.
	v, ok := b.Get(ctx, "k")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), v)

	a.Set(ctx, "k", []byte("v2"), time.Minute)

	assert.Eventually(t, func() bool {
		v, ok := b.Get(ctx, "k")
		return ok && string(v) == "v2"
	}, 500*time.Millisecond, 10*time.Millisecond, "B still serves stale v1 after A.Set(v2)")
}

// TestCache_SetBroadcastKeepsOwnL1 verifies that the setting instance does not
// drop its own freshly written L1 entry when its own broadcast loops back.
func TestCache_SetBroadcastKeepsOwnL1(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	c.Set(ctx, "k", []byte("v"), time.Minute)

	// Give the loopback pub/sub message time to arrive; the entry must survive.
	time.Sleep(200 * time.Millisecond)
	v, ok := c.Get(ctx, "k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), v)
}

// TestCache_L2Breaker_RedisDownNoLatencyCliff verifies that a Redis outage
// does not turn every cache operation into a per-request connection wait:
// after a few failures the L2 circuit breaker opens and Gets are served from
// L1 (or miss) fast. After Redis comes back the breaker closes again
// (half-open probe) within 30s.
func TestCache_L2Breaker_RedisDownNoLatencyCliff(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
	ctx := context.Background()

	// Bind Redis to a FIXED host port: Docker re-allocates random host ports
	// on container restart, but a real outage+recovery happens at a stable
	// address — which is exactly what the breaker's half-open probe needs.
	hostPort := freeLocalPort(t)
	rc, err := tcredis.Run(ctx, "redis:7-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.PortBindings = network.PortMap{
						network.MustParsePort("6379/tcp"): []network.PortBinding{
							{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(hostPort)},
						},
					}
				},
			},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rc.Terminate(context.Background())
	})
	addr := "127.0.0.1:" + strconv.Itoa(hostPort)

	c := newCacheAt(t, addr)

	// Warm an L1 entry so we can prove L1 keeps serving during the outage.
	c.Set(ctx, "warm", []byte("v"), time.Minute)

	// Kill Redis.
	stopTimeout := 2 * time.Second
	require.NoError(t, rc.Stop(ctx, &stopTimeout))

	// Trip the breaker: a handful of sacrificial L2 misses (untimed — the
	// first few may pay a dial error each).
	for i := range 10 {
		_, _ = c.Get(ctx, fmt.Sprintf("trip-%d", i))
	}

	// With the breaker open, 100 distinct-key Gets (all L2-path misses) must
	// each complete fast — no per-op connection wait.
	for i := range 100 {
		start := time.Now()
		_, ok := c.Get(ctx, fmt.Sprintf("down-%d", i))
		elapsed := time.Since(start)
		assert.False(t, ok)
		require.Less(t, elapsed, 50*time.Millisecond, "Get %d took %v with Redis down (breaker not open?)", i, elapsed)
	}

	// L1 keeps serving the warm entry.
	v, ok := c.Get(ctx, "warm")
	require.True(t, ok, "L1 must keep serving during the outage")
	require.Equal(t, []byte("v"), v)

	// Bring Redis back (same container keeps its mapped port).
	require.NoError(t, rc.Start(ctx))

	// The breaker must close again (half-open probe succeeds) and L2 must be
	// written through once more: a second instance observes the Set.
	b := newCacheAt(t, addr)
	i := 0
	require.Eventually(t, func() bool {
		i++
		key := fmt.Sprintf("recovered-%d", i)
		c.Set(ctx, key, []byte("back"), time.Minute)
		_, ok := b.Get(ctx, key)
		return ok
	}, 30*time.Second, 1*time.Second, "breaker did not recover within 30s of Redis restart")
}

// TestCache_Close_NoSubscriberLeak verifies the pub/sub subscriber goroutine
// exits on Close (goleak).
func TestCache_Close_NoSubscriberLeak(t *testing.T) {
	addr := newRedisContainer(t)

	// Snapshot goroutines AFTER the container infra is up so testcontainers'
	// own background goroutines are excluded from the leak check.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	cfg := cache.Config{
		RedisAddrs: []string{addr},
		L1Capacity: 10,
		DefaultTTL: time.Minute,
	}
	c, err := cache.New(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	c.Set(ctx, "k", []byte("v"), time.Minute)
	require.NoError(t, c.Delete(ctx, "k"))
	require.NoError(t, c.Close(ctx))
}
