package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// readyzResponse is the JSON shape written by ReadyzHandler.
type readyzResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

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

// ReadyzHandler runs all readiness checks concurrently; 200 only if ready and
// all pass. Each check is bounded by its own per-check timeout (default 2s).
//
// The handler itself returns at the per-check deadline regardless of whether
// the check honours its context — a check goroutine that ignores ctx may linger,
// but it cannot block the HTTP handler beyond the timeout. A panicking check is
// treated as a failure and never crashes the handler. The response body is
// always valid JSON describing the status of every check.
func (h *Health) ReadyzHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Shutdown gating: short-circuit immediately if not ready.
		if !h.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, readyzResponse{
				Status: "unavailable",
				Checks: map[string]string{},
			})
			return
		}

		// Snapshot checks and timeout under RLock.
		h.mu.RLock()
		timeout := h.checkTimeout
		checks := make(map[string]Check, len(h.checks))
		for k, v := range h.checks {
			checks[k] = v
		}
		h.mu.RUnlock()

		// Pre-allocate result storage. Each goroutine writes to its own
		// index in the slice to avoid concurrent map writes.
		type result struct {
			name string
			err  string // empty string means "ok"
		}
		results := make([]result, 0, len(checks))
		checkFns := make([]Check, 0, len(checks))
		for name, c := range checks {
			results = append(results, result{name: name})
			checkFns = append(checkFns, c)
		}

		var wg sync.WaitGroup
		for idx, c := range checkFns {
			wg.Add(1)
			go func(idx int, c Check) {
				defer wg.Done()

				checkCtx, cancel := context.WithTimeout(r.Context(), timeout)
				defer cancel()

				// Race the check against its own deadline. If the check ignores
				// ctx and blocks, the handler still returns at the deadline rather
				// than blocking on wg.Wait(). The lingering goroutine is
				// unavoidable in Go but does not affect handler latency.
				done := make(chan error, 1)
				go func() {
					defer func() {
						if p := recover(); p != nil {
							done <- fmt.Errorf("panic: %v", p)
						}
					}()
					done <- c(checkCtx)
				}()

				select {
				case err := <-done:
					if err != nil {
						results[idx].err = err.Error()
					}
				case <-checkCtx.Done():
					results[idx].err = "timeout"
				}
			}(idx, c)
		}
		wg.Wait()

		// Assemble JSON body and determine overall status.
		checksMap := make(map[string]string, len(results))
		allOK := true
		for _, res := range results {
			if res.err != "" {
				checksMap[res.name] = res.err
				allOK = false
			} else {
				checksMap[res.name] = "ok"
			}
		}

		if allOK {
			writeJSON(w, http.StatusOK, readyzResponse{
				Status: "ok",
				Checks: checksMap,
			})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, readyzResponse{
				Status: "unavailable",
				Checks: checksMap,
			})
		}
	})
}

// writeJSON encodes v as JSON and writes it with the given status code.
// Content-Type is set to application/json.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Mount registers GET /livez and GET /readyz on the given chi router.
// It wires h.LivezHandler() and h.ReadyzHandler() respectively so that a
// service can expose health endpoints with a single call:
//
//	health.Mount(server.Mux(), h)
//
// The registered routes use chi's method-scoped handler registration, which
// means they participate in chi's middleware stack normally.
func Mount(r interface {
	Get(pattern string, handlerFn http.HandlerFunc)
}, h *Health,
) {
	r.Get("/livez", h.LivezHandler().ServeHTTP)
	r.Get("/readyz", h.ReadyzHandler().ServeHTTP)
}
