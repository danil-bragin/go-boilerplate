package resilience_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/resilience"
)

// TestRetry_RetriesThenSucceeds verifies that the retry policy retries a
// transiently-failing function and reports success once it recovers.
func TestRetry_RetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32

	fn := func(_ context.Context) error {
		n := counter.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}

	err := resilience.Do(ctx, fn, resilience.Retry(5, time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, int32(3), counter.Load(), "expected exactly 3 calls (2 failures + 1 success)")
}

// TestRetry_GivesUpAfterMaxAttempts verifies that the retry policy stops after
// maxAttempts and surfaces the last error.
func TestRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32
	sentinel := errors.New("always fails")

	fn := func(_ context.Context) error {
		counter.Add(1)
		return sentinel
	}

	err := resilience.Do(ctx, fn, resilience.Retry(3, time.Millisecond))
	require.Error(t, err)
	assert.Equal(t, int32(3), counter.Load(), "expected exactly 3 attempts")
}

// TestCircuitBreaker_OpensAfterThreshold verifies that after failureThreshold
// consecutive failures the circuit opens and subsequent calls fail fast
// without invoking fn again.
func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32
	sentinel := errors.New("failure")

	cb := resilience.CircuitBreaker(2, time.Minute)

	fn := func(_ context.Context) error {
		counter.Add(1)
		return sentinel
	}

	// First 2 calls should trip the breaker.
	_ = resilience.Do(ctx, fn, cb)
	_ = resilience.Do(ctx, fn, cb)

	assert.Equal(t, circuitbreaker.OpenState, cb.State(), "circuit should be open after threshold failures")

	counterAtOpen := counter.Load()

	// All subsequent calls should fail fast with ErrOpen, not invoking fn.
	for i := 0; i < 3; i++ {
		err := resilience.Do(ctx, fn, cb)
		require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	}

	assert.Equal(t, counterAtOpen, counter.Load(), "fn must not be called when circuit is open")
}

// TestTimeout_CancelsSlowFn verifies that a timeout policy aborts a slow
// function well before it would complete on its own. The function uses
// context-aware sleeping so the timeout cancellation propagates.
func TestTimeout_CancelsSlowFn(t *testing.T) {
	ctx := context.Background()

	fn := func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	start := time.Now()
	err := resilience.Do(ctx, fn, resilience.Timeout(20*time.Millisecond))
	elapsed := time.Since(start)

	require.Error(t, err, "expected a timeout error")
	assert.Less(t, elapsed, 150*time.Millisecond, "execution should have been cut short")
}

// TestGet_ReturnsTypedResult verifies the generic Get helper boxes and
// unboxes the result correctly.
func TestGet_ReturnsTypedResult(t *testing.T) {
	ctx := context.Background()

	fn := func(_ context.Context) (int, error) {
		return 42, nil
	}

	val, err := resilience.Get[int](ctx, fn, resilience.Retry(2, time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, 42, val)
}

// TestBulkhead_LimitsConcurrency verifies that at most maxConcurrent goroutines
// execute fn at the same time. Goroutines that exceed the limit receive
// bulkhead.ErrFull and are not counted.
func TestBulkhead_LimitsConcurrency(t *testing.T) {
	ctx := context.Background()
	const maxConcurrent = 3
	const totalGoroutines = 10

	bh := resilience.Bulkhead(maxConcurrent)

	var (
		current atomic.Int32
		peak    atomic.Int32
	)

	ready := make(chan struct{})

	fn := func(_ context.Context) error {
		n := current.Add(1)
		defer current.Add(-1)

		// Track peak concurrency.
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}

		<-ready // hold slot until all goroutines have started
		return nil
	}

	errCh := make(chan error, totalGoroutines)
	for i := 0; i < totalGoroutines; i++ {
		go func() {
			errCh <- resilience.Do(ctx, fn, bh)
		}()
	}

	// Give goroutines time to fill the bulkhead.
	time.Sleep(20 * time.Millisecond)
	close(ready) // release held goroutines

	// Drain results.
	for i := 0; i < totalGoroutines; i++ {
		<-errCh
	}

	assert.LessOrEqual(t, peak.Load(), int32(maxConcurrent),
		"peak concurrency must not exceed bulkhead limit")
}

// TestRateLimiter_Allow verifies basic token-bucket behaviour: the first
// Allow succeeds and an immediate second one is rejected.
func TestRateLimiter_Allow(t *testing.T) {
	rl := resilience.NewRateLimiter(1, 1) // 1 token/s, burst=1

	assert.True(t, rl.Allow(), "first Allow should consume the initial burst token")
	assert.False(t, rl.Allow(), "second immediate Allow should be rejected (no tokens)")
}
