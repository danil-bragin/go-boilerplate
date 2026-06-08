// Package health provides Kubernetes-style liveness (/livez) and readiness
// (/readyz) endpoints. Liveness reflects process health only; readiness
// aggregates dependency checks and is flipped to "not ready" on shutdown.
package health

import (
	"context"
	"net/http"
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

	mu     sync.RWMutex
	checks map[string]Check
}

// New returns a Health that starts live and ready with no checks.
func New() *Health {
	h := &Health{checks: make(map[string]Check)}
	h.live.Store(true)
	h.ready.Store(true)
	return h
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

// LivezHandler serves liveness.
func (h *Health) LivezHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if h.live.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
}

// ReadyzHandler runs all readiness checks; 200 only if ready and all pass.
func (h *Health) ReadyzHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		h.mu.RLock()
		checks := make(map[string]Check, len(h.checks))
		for k, v := range h.checks {
			checks[k] = v
		}
		h.mu.RUnlock()

		for _, c := range checks {
			if err := c(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
