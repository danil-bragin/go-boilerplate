package cache

import (
	"context"
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

	c.subWG.Add(1)
	go func() {
		defer c.subWG.Done()
		for {
			err := c.l2.Receive(subCtx, c.l2.B().Subscribe().Channel(c.invChannel()).Build(), func(msg rueidis.PubSubMessage) {
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
			if subCtx.Err() != nil || err == nil {
				return
			}
			// Transient Redis error — back off and resubscribe. Broadcasts
			// published while disconnected are lost; staleness is bounded by
			// the entry TTL (same eventual-consistency floor as before).
			select {
			case <-subCtx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
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

// publishInvalidation broadcasts key on the invalidation channel. Receivers
// other than this instance drop the key from their L1. Errors are ignored:
// a failed broadcast degrades to TTL-bounded staleness, never a caller error.
func (c *Cache) publishInvalidation(ctx context.Context, key string) {
	payload := c.instanceID + " " + key
	_ = c.l2.Do(ctx, c.l2.B().Publish().Channel(c.invChannel()).Message(payload).Build()).Error()
}

// stopInvalidationSubscriber cancels the subscriber goroutine and waits for
// it to exit.
func (c *Cache) stopInvalidationSubscriber() {
	if c.subCancel != nil {
		c.subCancel()
	}
	c.subWG.Wait()
}
