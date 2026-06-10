package cache

// Internal (white-box) tests for the invalidation subscribe loop's
// resubscribe semantics. No Redis needed: the receive seam is stubbed.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLoopCache builds a Cache with only the fields subscribeLoop touches:
// an L1 and the given receive stub.
func newLoopCache(t *testing.T, receive func(ctx context.Context, fn func(msg rueidis.PubSubMessage)) error) *Cache {
	t.Helper()
	l1, err := otter.New[string, []byte](&otter.Options[string, []byte]{MaximumSize: 100})
	require.NoError(t, err)
	t.Cleanup(func() { l1.StopAllGoroutines() })
	return &Cache{l1: l1, receive: receive}
}

// runLoop starts subscribeLoop in the background and returns a cancel that
// also waits for it to exit.
func runLoop(t *testing.T, c *Cache) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.subscribeLoop(ctx, func(rueidis.PubSubMessage) {})
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("subscribeLoop did not exit after cancel")
		}
	}
	t.Cleanup(stop)
	return stop
}

// TestSubscribeLoop_NilReturnResubscribes verifies that a nil return from
// receive — rueidis Receive returns nil on ANY unsubscribe message, including
// a SERVER-INITIATED one — is treated as a subscription gap (resubscribe),
// not as a clean shutdown. Treating nil as terminal kills cross-instance L1
// invalidation silently for the life of the process.
func TestSubscribeLoop_NilReturnResubscribes(t *testing.T) {
	calls := make(chan int, 16)
	n := 0
	c := newLoopCache(t, func(ctx context.Context, _ func(msg rueidis.PubSubMessage)) error {
		n++
		calls <- n
		if n == 1 {
			return nil // server-initiated unsubscribe
		}
		<-ctx.Done()
		return ctx.Err()
	})

	runLoop(t, c)

	require.Eventually(t, func() bool {
		select {
		case got := <-calls:
			return got >= 2
		default:
			return false
		}
	}, 3*time.Second, 10*time.Millisecond,
		"loop must resubscribe after a nil (server-unsubscribe) return")
}

// TestSubscribeLoop_GapColdsL1 verifies that any resubscribe gap (transport
// error here) drops the WHOLE L1 before resuming: broadcasts published during
// the gap were lost, so any entry cached before the gap may be stale and must
// not outlive the missed invalidations.
func TestSubscribeLoop_GapColdsL1(t *testing.T) {
	resubscribed := make(chan struct{})
	n := 0
	c := newLoopCache(t, func(ctx context.Context, _ func(msg rueidis.PubSubMessage)) error {
		n++
		if n == 1 {
			return errors.New("connection reset")
		}
		close(resubscribed)
		<-ctx.Done()
		return ctx.Err()
	})
	c.l1.Set("stale-during-gap", []byte("v"))

	runLoop(t, c)

	select {
	case <-resubscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("loop did not resubscribe after a transport error")
	}
	_, ok := c.l1.GetIfPresent("stale-during-gap")
	assert.False(t, ok, "L1 must be cold after a subscription gap — a missed broadcast may have invalidated any entry")
}

// TestSubscribeLoop_ErrClosingStops verifies that a closed rueidis client
// terminates the loop instead of hot-looping resubscribe attempts forever.
func TestSubscribeLoop_ErrClosingStops(t *testing.T) {
	c := newLoopCache(t, func(_ context.Context, _ func(msg rueidis.PubSubMessage)) error {
		return rueidis.ErrClosing
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.subscribeLoop(ctx, func(rueidis.PubSubMessage) {})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop must stop when the client is closing")
	}
}
