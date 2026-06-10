package cache

// Internal (white-box) tests for the L2 circuit breaker's failure
// attribution. No Redis needed: l2Done/l2Allowed are pure breaker
// bookkeeping.

import (
	"context"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBreakerOnlyCache() *Cache {
	c := &Cache{}
	c.l2cb, c.l2cbGauge = newL2Breaker()
	return c
}

// TestL2Done_CallerCancellationDoesNotOpenBreaker verifies that an L2 op that
// failed because the CALLER's context was cancelled is not counted as an L2
// failure: a burst of cancelled requests against healthy Redis must not open
// the breaker and cut every instance over to L1-only mode.
func TestL2Done_CallerCancellationDoesNotOpenBreaker(t *testing.T) {
	c := newBreakerOnlyCache()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for range 3 * l2BreakerFailureThreshold {
		c.l2Done(cancelled, context.Canceled)
	}
	assert.Equal(t, circuitbreaker.ClosedState, c.l2cb.State(),
		"caller-cancelled ops must not count as L2 failures")
}

// TestL2Done_CallerDeadlineDoesNotOpenBreaker covers the caller's own
// deadline firing before/with the op: also caller-driven, not Redis health.
func TestL2Done_CallerDeadlineDoesNotOpenBreaker(t *testing.T) {
	c := newBreakerOnlyCache()

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	for range 3 * l2BreakerFailureThreshold {
		c.l2Done(expired, context.DeadlineExceeded)
	}
	assert.Equal(t, circuitbreaker.ClosedState, c.l2cb.State(),
		"caller-deadline ops must not count as L2 failures")
}

// TestL2Done_OpTimeoutStillOpensBreaker verifies the flip side: an op that
// hit the L2OpTimeout bound while the CALLER was still live is a genuine L2
// failure (slow/unresponsive Redis) and must keep tripping the breaker.
func TestL2Done_OpTimeoutStillOpensBreaker(t *testing.T) {
	c := newBreakerOnlyCache()
	ctx := context.Background() // caller alive — the deadline came from l2ctx

	for range l2BreakerFailureThreshold {
		c.l2Done(ctx, context.DeadlineExceeded)
	}
	assert.Equal(t, circuitbreaker.OpenState, c.l2cb.State(),
		"op-timeouts with a live caller must open the breaker")
}

// TestL2Done_HalfOpenProbeAlwaysRecords verifies the half-open special case:
// the probe permit acquired by l2Allowed MUST be released by recording a
// result — otherwise cancelled callers during half-open would leak all probe
// permits and wedge the breaker half-open forever, with no probes possible.
func TestL2Done_HalfOpenProbeAlwaysRecords(t *testing.T) {
	c := newBreakerOnlyCache()
	c.l2cb.HalfOpen()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	// Half-open grants l2BreakerFailureThreshold probe permits; recording a
	// result releases the permit. If l2Done skipped recording for cancelled
	// callers here, every cycle would leak one permit and the breaker would
	// wedge half-open forever (acquires keep succeeding until the pool is
	// empty, but the state never moves). With the conservative failure
	// record, each permit is released and the failure threshold eventually
	// re-opens the breaker.
	for i := range int(l2BreakerFailureThreshold) {
		require.True(t, c.l2cb.TryAcquirePermit(),
			"probe permit %d must be available (released by the previous record)", i)
		c.l2Done(cancelled, context.Canceled)
	}
	assert.Equal(t, circuitbreaker.OpenState, c.l2cb.State(),
		"cancelled half-open probes must record results — the breaker must re-open, not wedge half-open")
}

// TestL2Allowed_CancelledCallerSkipsL2 verifies that an already-cancelled
// caller never touches L2 at all: no breaker permit is consumed and no
// half-open probe is wasted on a request that cannot complete.
func TestL2Allowed_CancelledCallerSkipsL2(t *testing.T) {
	c := newBreakerOnlyCache()
	c.l2cb.HalfOpen()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, c.l2Allowed(cancelled), "cancelled caller must skip the L2 path")
	assert.Equal(t, circuitbreaker.HalfOpenState, c.l2cb.State())
	// The single half-open probe permit must still be available for a live caller.
	assert.True(t, c.l2cb.TryAcquirePermit(),
		"cancelled callers must not consume half-open probe permits")
}
