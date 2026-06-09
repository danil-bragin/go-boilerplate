package cqrs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/cqrs"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
)

// fakeCache is an in-memory Cache implementation for tests. GetOrLoad
// collapses concurrent misses via singleflight, mirroring the real
// platform/storage/cache implementation.
type fakeCache struct {
	mu    sync.Mutex
	sf    singleflight.Group
	store map[string][]byte
}

func newFakeCache() *fakeCache {
	return &fakeCache{store: make(map[string][]byte)}
}

func (c *fakeCache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.store[key]
	return v, ok
}

func (c *fakeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = value
}

func (c *fakeCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, key)
	return nil
}

func (c *fakeCache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok := c.Get(ctx, key); ok {
		return v, nil
	}
	v, err, _ := c.sf.Do(key, func() (any, error) {
		if v, ok := c.Get(ctx, key); ok {
			return v, nil
		}
		b, err := load(ctx)
		if err != nil {
			return nil, err
		}
		c.Set(ctx, key, b, ttl)
		return b, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil //nolint:forcetypeassert
}

type (
	cacheQuery  struct{ ID string }
	cacheResult struct{ Value string }
)

// TestCaching_ReturnsCachedOnHitAndSkipsHandler verifies the cache-aside
// pattern: first call is a miss → handler runs; second call with the same key
// is a hit → handler is NOT called again and the same result is returned.
func TestCaching_ReturnsCachedOnHitAndSkipsHandler(t *testing.T) {
	cache := newFakeCache()
	var callCount int

	handler := cqrs.HandlerFunc[cacheQuery, cacheResult](func(_ context.Context, q cacheQuery) (cacheResult, error) {
		callCount++
		return cacheResult{Value: "hello-" + q.ID}, nil
	})

	keyFor := func(q cacheQuery) string { return q.ID }
	decorated := cqrs.Decorate(handler, cqrs.CachingJSON[cacheQuery, cacheResult](cache, keyFor, time.Minute))

	ctx := context.Background()
	q := cacheQuery{ID: "item-1"}

	// First call — cache miss, handler runs.
	res1, err := decorated(ctx, q)
	require.NoError(t, err)
	require.Equal(t, cacheResult{Value: "hello-item-1"}, res1)
	require.Equal(t, 1, callCount)

	// Second call — cache hit, handler must NOT run.
	res2, err := decorated(ctx, q)
	require.NoError(t, err)
	require.Equal(t, res1, res2)
	require.Equal(t, 1, callCount, "handler must not be called on cache hit")
}

// TestCaching_HandlerErrorNotCached verifies that when the handler returns an
// error the result is not stored in the cache, so the next call retries the
// handler.
func TestCaching_HandlerErrorNotCached(t *testing.T) {
	cache := newFakeCache()
	var callCount int
	boom := errors.New("handler error")

	handler := cqrs.HandlerFunc[cacheQuery, cacheResult](func(_ context.Context, _ cacheQuery) (cacheResult, error) {
		callCount++
		return cacheResult{}, boom
	})

	keyFor := func(q cacheQuery) string { return q.ID }
	decorated := cqrs.Decorate(handler, cqrs.CachingJSON[cacheQuery, cacheResult](cache, keyFor, time.Minute))

	ctx := context.Background()
	q := cacheQuery{ID: "item-2"}

	_, err := decorated(ctx, q)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, callCount)

	// Second call — nothing cached, handler must run again.
	_, err = decorated(ctx, q)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 2, callCount, "handler must be called again after prior error")
}

// TestCaching_MissOnUnmarshalGarbage verifies that garbage bytes pre-seeded in
// the cache are treated as a miss: the handler runs and the cache is
// overwritten with valid data.
func TestCaching_MissOnUnmarshalGarbage(t *testing.T) {
	cache := newFakeCache()
	var callCount int

	handler := cqrs.HandlerFunc[cacheQuery, cacheResult](func(_ context.Context, q cacheQuery) (cacheResult, error) {
		callCount++
		return cacheResult{Value: "fresh-" + q.ID}, nil
	})

	keyFor := func(q cacheQuery) string { return q.ID }
	decorated := cqrs.Decorate(handler, cqrs.CachingJSON[cacheQuery, cacheResult](cache, keyFor, time.Minute))

	ctx := context.Background()
	q := cacheQuery{ID: "item-3"}

	// Pre-seed garbage bytes for the key.
	cache.Set(ctx, q.ID, []byte("not valid json {{{}"), time.Minute)

	// Call — unmarshal fails → treated as miss → handler runs.
	res, err := decorated(ctx, q)
	require.NoError(t, err)
	require.Equal(t, cacheResult{Value: "fresh-item-3"}, res)
	require.Equal(t, 1, callCount, "handler must run on unmarshal failure (cache miss)")

	// Cache should now hold valid data; second call is a hit.
	res2, err := decorated(ctx, q)
	require.NoError(t, err)
	require.Equal(t, res, res2)
	require.Equal(t, 1, callCount, "handler must not run again after successful cache write")
}

// TestCaching_ConcurrentMissesCollapseToOneHandlerCall verifies that the
// Caching behavior routes misses through Cache.GetOrLoad so that a
// singleflight-capable cache collapses N concurrent misses into ONE handler
// invocation (stampede protection is reachable from the CQRS pipeline).
func TestCaching_ConcurrentMissesCollapseToOneHandlerCall(t *testing.T) {
	cache := newFakeCache()
	var calls atomic.Int64

	handler := cqrs.HandlerFunc[cacheQuery, cacheResult](func(_ context.Context, q cacheQuery) (cacheResult, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond) // widen the in-flight window
		return cacheResult{Value: "hot-" + q.ID}, nil
	})

	keyFor := func(q cacheQuery) string { return q.ID }
	decorated := cqrs.Decorate(handler, cqrs.CachingJSON[cacheQuery, cacheResult](cache, keyFor, time.Minute))

	const goroutines = 50
	var wg sync.WaitGroup
	results := make([]cacheResult, goroutines)
	errs := make([]error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = decorated(context.Background(), cacheQuery{ID: "hot"})
		}()
	}
	wg.Wait()

	for i := range goroutines {
		require.NoError(t, errs[i])
		require.Equal(t, cacheResult{Value: "hot-hot"}, results[i], "goroutine %d", i)
	}
	require.Equal(t, int64(1), calls.Load(), "handler must run exactly once for concurrent misses")
}
