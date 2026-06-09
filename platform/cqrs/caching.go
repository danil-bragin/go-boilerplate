package cqrs

import (
	"context"
	"encoding/json"
	"time"
)

// Cache is the minimal cache-aside contract the Caching behavior needs.
// Get returns (value, true) on hit. Set stores value with a ttl.
// Delete removes the key from every tier; distributed implementations must
// also broadcast the eviction to other instances (write paths call Delete to
// bust stale read-model entries).
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration)
	Delete(ctx context.Context, key string) error
}

// Codec encodes/decodes the result R for caching.
type Codec[R any] interface {
	Marshal(R) ([]byte, error)
	Unmarshal([]byte) (R, error)
}

// JSONCodec is a Codec using encoding/json.
type JSONCodec[R any] struct{}

// Marshal serialises r to JSON.
func (JSONCodec[R]) Marshal(r R) ([]byte, error) { return json.Marshal(r) }

// Unmarshal deserialises b from JSON into R.
func (JSONCodec[R]) Unmarshal(b []byte) (R, error) {
	var r R
	err := json.Unmarshal(b, &r)
	return r, err
}

// Caching returns a cache-aside Behavior for QUERY handlers. On a cache hit
// the handler is skipped entirely. On a miss the handler is called and, on
// success, the result is serialised with codec and stored in cache. Caching
// errors (marshal / set) are silently discarded so they never fail the query.
// Apply this ONLY to queries; commands must NOT use this behavior.
//
// Stampede / single-flight: Caching provides NO single-flight protection.
// N concurrent misses for the same key will all invoke the handler; each
// successful response is written to the cache independently. Callers that
// need stampede protection (singleflight) must wrap this behavior or the
// cache implementation themselves.
//
// On handler error: the (possibly zero) result is returned uncached, consistent
// with the Transaction behavior that also returns zero,err on failure.
func Caching[C, R any](cache Cache, keyFor func(C) string, ttl time.Duration, codec Codec[R]) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			key := keyFor(cmd)

			if raw, ok := cache.Get(ctx, key); ok {
				if r, err := codec.Unmarshal(raw); err == nil {
					return r, nil
				}
				// Unmarshal failed — treat as cache miss and fall through.
			}

			res, err := next(ctx, cmd)
			if err != nil {
				return res, err
			}

			// Best-effort cache write; ignore marshal / set errors.
			if raw, merr := codec.Marshal(res); merr == nil {
				cache.Set(ctx, key, raw, ttl)
			}
			return res, nil
		}
	}
}

// CachingJSON is a convenience wrapper around Caching that uses JSONCodec[R].
func CachingJSON[C, R any](cache Cache, keyFor func(C) string, ttl time.Duration) Behavior[C, R] {
	return Caching[C, R](cache, keyFor, ttl, JSONCodec[R]{})
}
