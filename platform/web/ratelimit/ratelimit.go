// Package ratelimit provides per-key rate limiting for the HTTP edge.
// Limiter implementations: NewMemory (single instance, zero deps) and
// NewRedis (distributed across replicas via an atomic Lua token bucket).
package ratelimit

import (
	"context"
	"time"
)

// Result is the outcome of a rate-limit decision. Beyond the allow/deny bit
// it carries the data clients need to behave well: the bucket capacity
// (RateLimit-Limit header), the remaining budget (RateLimit-Remaining), and —
// when denied — how long to wait before retrying (Retry-After).
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the bucket capacity (burst). 0 when unknown.
	Limit int64
	// Remaining is the number of tokens left after this decision.
	// -1 means unknown (e.g. a fail-open decision while Redis is down).
	Remaining int64
	// RetryAfter is the wait until the next token becomes available.
	// Only meaningful when Allowed is false; 0 otherwise.
	RetryAfter time.Duration
}

// Limiter reports whether the caller identified by key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}
