// Package health provides Kubernetes-style liveness (/livez) and readiness
// (/readyz) endpoints. Liveness reflects process health only; readiness
// aggregates dependency checks and is flipped to "not ready" on shutdown.
package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Check probes a dependency; returns nil when healthy.
type Check func(ctx context.Context) error

// Health aggregates liveness and readiness state.
type Health struct {
	live  atomic.Bool
	ready atomic.Bool

	mu           sync.RWMutex
	checks       map[string]Check
	checkTimeout time.Duration
}

// New returns a Health that starts live and ready with no checks.
// The default per-check timeout is 2s; override with SetCheckTimeout.
func New() *Health {
	h := &Health{
		checks:       make(map[string]Check),
		checkTimeout: 2 * time.Second,
	}
	h.live.Store(true)
	h.ready.Store(true)
	return h
}

// SetCheckTimeout configures the per-check timeout applied when running
// readiness checks concurrently. Each check gets its own deadline derived
// from the request context bounded by d. Defaults to 2s.
func (h *Health) SetCheckTimeout(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkTimeout = d
}

// AddReadiness registers a named readiness check.
func (h *Health) AddReadiness(name string, c Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = c
}

// SetNotLive marks the process as not live (e.g. fatal internal error).
func (h *Health) SetNotLive() { h.live.Store(false) }

// SetNotReady marks the service as not ready (call at the start of shutdown).
func (h *Health) SetNotReady() { h.ready.Store(false) }
