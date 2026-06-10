package servicekit

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// AddPeriodicWorker registers a named ticker-driven background worker under
// the service lifecycle: Start launches it, teardown cancels and waits for it
// (same wg/closer machinery as every other harness goroutine).
//
// Every interval the worker sleeps an extra random delay in [0, jitter)
// (jitter 0 = none) and then invokes fn. fn errors are LOGGED, never fatal —
// the loop keeps ticking. fn must return promptly when its ctx is cancelled.
//
// With singleActive=true the loop runs under pg.RunAsLeader: a Postgres
// advisory lock (keyed by name, schema-scoped) guarantees that across all
// instances of the service AT MOST ONE runs the worker at a time, with
// automatic failover when the leader dies (see pg.RunAsLeader for the
// election/fencing semantics — brief overlap is possible, so fn must still
// be idempotent). singleActive requires Postgres: an error is returned when
// the service was built with WithoutPG.
//
// With singleActive=false EVERY instance runs the worker. Use this for loops
// that are naturally concurrency-safe — e.g. the harness's inbox and audit
// cleaners, whose age-based DELETEs are idempotent (concurrent runs just
// delete disjoint or already-gone rows).
//
// Must be called before Start.
func (s *Service) AddPeriodicWorker(name string, interval, jitter time.Duration, singleActive bool, fn func(context.Context) error) error {
	if interval <= 0 {
		return fmt.Errorf("servicekit: AddPeriodicWorker %q: interval must be positive, got %v", name, interval)
	}
	if singleActive && s.pool == nil {
		return errors.New("servicekit: AddPeriodicWorker with singleActive requires postgres, but the service was built with WithoutPG()")
	}

	loop := s.periodicLoop(name, interval, jitter, fn)
	run := loop
	if singleActive {
		lockPool := s.pool.Writer()
		run = func(ctx context.Context) error {
			return pg.RunAsLeader(ctx, lockPool, name, loop)
		}
	}

	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		s.logger.Debug("periodic worker started",
			"worker", name, "interval", interval, "jitter", jitter, "single_active", singleActive)
		if err := run(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("periodic worker stopped unexpectedly", "worker", name, "error", err)
		}
	})
	return nil
}

// periodicLoop returns the ticker loop for one periodic worker: tick, sleep
// the per-tick jitter, run fn, log (not propagate) fn errors, exit on ctx
// cancellation.
func (s *Service) periodicLoop(name string, interval, jitter time.Duration, fn func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			if d := jitterDelay(jitter); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				s.logger.Error("periodic worker error", "worker", name, "error", err)
			}
		}
	}
}

// jitterDelay returns a uniformly random delay in [0, jitter), or 0 when
// jitter is not positive. Spreading ticks across replicas avoids synchronized
// thundering-herd load on shared dependencies.
func jitterDelay(jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(jitter))) //nolint:gosec // G404: jitter spread, not a security context
}
