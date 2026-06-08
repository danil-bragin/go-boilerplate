// Package run manages application lifecycle: ordered resource teardown
// and signal-driven graceful shutdown.
package run

import (
	"context"
	"errors"
	"sync"
)

// TeardownFunc releases a resource. It should respect ctx cancellation.
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
// that late-registered resources are never silently dropped.
func (c *Closer) Add(name string, fn TeardownFunc) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = fn(context.Background()) // closer already ran; clean up immediately
		return
	}
	c.items = append(c.items, teardown{name: name, fn: fn})
	c.mu.Unlock()
}

// Close runs every teardown in reverse order. All callbacks run even if some
// fail; the returned error joins all failures (with their resource names).
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
	for i := len(items) - 1; i >= 0; i-- {
		if err := items[i].fn(ctx); err != nil {
			errs = append(errs, errClose{name: items[i].name, err: err})
		}
	}
	return errors.Join(errs...)
}

type errClose struct {
	name string
	err  error
}

func (e errClose) Error() string { return e.name + ": " + e.err.Error() }
func (e errClose) Unwrap() error { return e.err }
