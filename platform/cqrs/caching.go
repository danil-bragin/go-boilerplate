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
// GetOrLoad returns the cached value or invokes load on a miss and caches the
// result. Implementations SHOULD collapse concurrent misses for the same key
// (singleflight) and SHOULD run load on a context detached from the first
// caller so one cancelled request cannot fail the collapsed waiters.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration)
	Delete(ctx context.Context, key string) error
	GetOrLoad(ctx context.Context, key string, ttl time.Duration, load func(ctx context.Context) ([]byte, error)) ([]byte, error)
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
// the handler is skipped entirely. On a miss the handler is called via
// Cache.GetOrLoad and, on success, the serialised result is stored in cache.
// Caching errors (marshal) are silently discarded so they never fail the
// query. Apply this ONLY to queries; commands must NOT use this behavior.
//
// Stampede / single-flight: misses are routed through Cache.GetOrLoad, so a
// singleflight-capable Cache (e.g. platform/storage/cache) collapses N
// concurrent misses for the same key into ONE handler invocation; collapsed
// waiters share the leader's result (and its error, if the handler failed).
//
// On handler error: the (possibly zero) result is returned uncached, consistent
// with the Transaction behavior that also returns zero,err on failure.
func Caching[C, R any](cache Cache, keyFor func(C) string, ttl time.Duration, codec Codec[R]) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			key := keyFor(cmd)

			// Captured only when THIS call's closure runs the handler
			// (it may not, on a hit or when collapsed onto another caller).
			var (
				res  R
				herr error
				ran  bool
			)

			raw, err := cache.GetOrLoad(ctx, key, ttl, func(lctx context.Context) ([]byte, error) {
				ran = true
				res, herr = next(lctx, cmd)
				if herr != nil {
					return nil, herr
				}
				return codec.Marshal(res)
			})
			if err != nil {
				if ran {
					// Handler failure propagates uncached; a marshal failure
					// is swallowed — the result is valid, just uncacheable.
					return res, herr
				}
				// Collapsed waiter sharing the leader's failure.
				var zero R
				return zero, err
			}

			if r, uerr := codec.Unmarshal(raw); uerr == nil {
				return r, nil
			}
			if ran {
				// Our own bytes failed to round-trip; the in-memory result
				// is still authoritative.
				return res, nil
			}
			// Garbage hit from the cache — treat as a miss: run the handler
			// directly and overwrite the entry best-effort.
			res, herr = next(ctx, cmd)
			if herr != nil {
				return res, herr
			}
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
