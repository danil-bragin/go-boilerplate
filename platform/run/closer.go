// Package run manages application lifecycle: ordered resource teardown
// and signal-driven graceful shutdown.
package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// TeardownFunc releases a resource. It should respect ctx cancellation.
//
// Convention: platform resources (pg.Pool, cache.Cache, kafka.Producer,
// kafka.Consumer, …) expose a Close(ctx context.Context) error method that
// matches this signature so they can be registered directly with Add:
//
//	closer.Add("kafka-consumer", consumer.Close)
type TeardownFunc func(ctx context.Context) error

type teardown struct {
	name string
	fn   TeardownFunc
}

// Closer collects teardown callbacks and runs them in reverse registration
// order (last registered closes first), aggregating any errors.
type Closer struct {
	mu     sync.Mutex
	items  []teardown
	closed bool
}

// NewCloser returns an empty Closer.
func NewCloser() *Closer { return &Closer{} }

// Add registers a named teardown callback. If Close has already been called,
// the teardown is run immediately (best-effort, using context.Background()) so
// that late-registered resources are never silently dropped; a failure on this
// late path is logged via slog.Default (Add has no error return and the
// Close() error has already been delivered, so logging is the only channel
// left to surface it).
func (c *Closer) Add(name string, fn TeardownFunc) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		// Closer already ran; clean up immediately.
		if err := fn(context.Background()); err != nil {
			slog.Default().Error("run: teardown added after Close failed",
				"resource", name, "error", err)
		}
		return
	}
	c.items = append(c.items, teardown{name: name, fn: fn})
	c.mu.Unlock()
}

// Close runs every teardown in reverse order. All callbacks run even if some
// fail; the returned error joins all failures (with their resource names).
//
// Teardown budget: ctx bounds the WHOLE teardown. When ctx expires midway,
// Close still attempts every remaining teardown (each callback observes the
// expired ctx and should bail out fast — best-effort cleanup beats leaked
// resources) and records the context error once in the returned error so the
// caller knows the budget was exhausted.
//
// Close is idempotent; subsequent calls are no-ops.
func (c *Closer) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	items := c.items
	c.items = nil
	c.mu.Unlock()

	var errs []error
	ctxErrRecorded := false
	for i := len(items) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil && !ctxErrRecorded {
			ctxErrRecorded = true
			errs = append(errs, fmt.Errorf("run: teardown budget exhausted before %q: %w", items[i].name, err))
		}
		if err := items[i].fn(ctx); err != nil {
			errs = append(errs, closeError{name: items[i].name, err: err})
		}
	}
	return errors.Join(errs...)
}

type closeError struct {
	name string
	err  error
}

func (e closeError) Error() string { return e.name + ": " + e.err.Error() }
func (e closeError) Unwrap() error { return e.err }
