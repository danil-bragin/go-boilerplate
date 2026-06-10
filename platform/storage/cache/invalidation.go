package cache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

// readyProbeKey is a sentinel key published by an instance to its own
// invalidation channel at startup. Receiving it back confirms the
// subscription is live, so subsequent broadcasts are not lost.
const readyProbeKey = "\x00cache-inv-ready"

// invChannel returns the Redis pub/sub channel used for cross-instance L1
// invalidation. Instances sharing the same InvalidationPrefix form one
// coherence domain.
func (c *Cache) invChannel() string {
	return "cache:inv:" + c.cfg.InvalidationPrefix
}

// startInvalidationSubscriber launches the background goroutine that listens
// on the invalidation channel and drops L1 entries named by other instances.
// It returns once the subscription is confirmed live (via a self-probe) or
// after a short timeout, so callers may rely on broadcasts published after
// New returns being observed.
func (c *Cache) startInvalidationSubscriber() {
	c.instanceID = uuid.NewString()

	subCtx, cancel := context.WithCancel(context.Background())
	c.subCancel = cancel

	ready := make(chan struct{})
	var readyOnce sync.Once

	if c.receive == nil {
		c.receive = func(ctx context.Context, fn func(msg rueidis.PubSubMessage)) error {
			return c.l2.Receive(ctx, c.l2.B().Subscribe().Channel(c.invChannel()).Build(), fn)
		}
	}

	c.subWG.Add(1)
	go func() {
		defer c.subWG.Done()
		c.subscribeLoop(subCtx, func(msg rueidis.PubSubMessage) {
			id, key, ok := strings.Cut(msg.Message, " ")
			if !ok {
				return // malformed payload — ignore
			}
			if id == c.instanceID {
				// Our own broadcast looping back. The local L1 already
				// holds the fresh state (Set) or was already evicted
				// (Delete) — only signal readiness for the self-probe.
				if key == readyProbeKey {
					readyOnce.Do(func() { close(ready) })
				}
				return
			}
			if key == readyProbeKey {
				return // another instance's startup probe
			}
			c.l1.Invalidate(key)
		})
	}()

	// Confirm the subscription is live before returning: publish a self-probe
	// until it loops back. Best-effort — on timeout the cache still works,
	// with broadcast delivery starting slightly later.
	deadline := time.After(3 * time.Second)
	for {
		c.publishInvalidation(subCtx, readyProbeKey)
		select {
		case <-ready:
			return
		case <-deadline:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// subscribeLoop runs the blocking receive/resubscribe loop until subCtx is
// done or the rueidis client is closed.
//
// Receive returning is ALWAYS a subscription gap, never a clean shutdown
// (those are the subCtx / ErrClosing cases): rueidis Receive returns nil on
// ANY unsubscribe message for the channel — including a server-initiated one
// — and an error on transport failures. Both must resubscribe; treating nil
// as terminal would kill cross-instance invalidation silently for the rest
// of the process lifetime.
//
// Every gap drops the WHOLE L1 before resuming: broadcasts published during
// the gap are lost, so any entry cached before the gap may already be stale
// and must not outlive the missed invalidations. (Entries written between
// the InvalidateAll and the new subscription going live keep the plain
// TTL-bounded staleness floor — the gap there is milliseconds, not the
// process lifetime.)
func (c *Cache) subscribeLoop(subCtx context.Context, handle func(msg rueidis.PubSubMessage)) {
	for {
		err := c.receive(subCtx, handle)
		if subCtx.Err() != nil || errors.Is(err, rueidis.ErrClosing) {
			return
		}
		// Back off briefly so repeated gaps cannot hot-loop SUBSCRIBE.
		select {
		case <-subCtx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
		c.l1.InvalidateAll()
	}
}

// publishInvalidation broadcasts key on the invalidation channel. Receivers
// other than this instance drop the key from their L1. Errors are ignored:
// a failed broadcast degrades to TTL-bounded staleness, never a caller error.
func (c *Cache) publishInvalidation(ctx context.Context, key string) {
	payload := c.instanceID + " " + key
	opCtx, opCancel := c.l2ctx(ctx)
	defer opCancel()
	_ = c.l2.Do(opCtx, c.l2.B().Publish().Channel(c.invChannel()).Message(payload).Build()).Error()
}

// stopInvalidationSubscriber cancels the subscriber goroutine and waits for
// it to exit.
func (c *Cache) stopInvalidationSubscriber() {
	if c.subCancel != nil {
		c.subCancel()
	}
	c.subWG.Wait()
}
