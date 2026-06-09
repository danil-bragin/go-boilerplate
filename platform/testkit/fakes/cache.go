package fakes

import (
	"context"
	"sync"
	"time"

	"go-boilerplate/platform/cqrs"
)

// Cache is an in-memory fake implementing cqrs.Cache.
// TTL values passed to Set are silently ignored.
type Cache struct {
	mu   sync.RWMutex
	data map[string][]byte
}

var _ cqrs.Cache = (*Cache)(nil)

// NewCache returns an initialised *Cache ready for use.
func NewCache() *Cache {
	return &Cache{data: make(map[string][]byte)}
}

// Get returns a copy of the stored value and true, or nil and false on a miss.
func (c *Cache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.RLock()
	v, ok := c.data[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

// Set stores a copy of value under key. ttl is accepted but ignored.
func (c *Cache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	cp := make([]byte, len(value))
	copy(cp, value)
	c.mu.Lock()
	c.data[key] = cp
	c.mu.Unlock()
}

// Delete removes key. It never fails.
func (c *Cache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
	return nil
}

// GetOrLoad returns the cached value or loads, stores and returns it on a
// miss. Unlike the real cache it performs NO singleflight collapsing —
// concurrent misses may each invoke load. Fine for tests; do not assert
// collapse behaviour against this fake.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok := c.Get(ctx, key); ok {
		return v, nil
	}
	v, err := load(ctx)
	if err != nil {
		return nil, err
	}
	c.Set(ctx, key, v, ttl)
	return v, nil
}
