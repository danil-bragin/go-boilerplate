// Package ratelimit provides per-key rate limiting for the HTTP edge.
// Limiter implementations: NewMemory (single instance, zero deps) and
// NewRedis (distributed across replicas via an atomic Lua token bucket).
package ratelimit

import "context"

// Limiter reports whether the caller identified by key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}
