// Package resilience provides thin builders for failsafe-go resilience policies and
// ergonomic execution helpers.
//
// All policy builders return Policy[any] so that heterogeneous slices can be passed to
// Do and Get without type-asserting each element. Get[T] boxes the typed result through
// any and type-asserts on return; the box is the only overhead.
//
// Bulkhead deviation: failsafe-go v0.9.6 ships a bulkhead sub-package (github.com/failsafe-go/failsafe-go/bulkhead)
// so the native failsafe Bulkhead[any] is used directly — no semaphore fallback needed.
package resilience

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/bulkhead"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

// Retry returns a RetryPolicy[any] with exponential back-off and TRUE full
// jitter (AWS style): each delay is drawn uniformly from [0, backoff] where
// backoff = min(32×baseDelay, baseDelay·2^retries).
//
// maxAttempts is the total number of attempts (1 = no retries).
//
// Implementation note: failsafe-go's WithJitterFactor(1.0) randomises the
// delay in [0, 2×computed_delay] — that is NOT full jitter (the window is
// twice as wide as the back-off, so worst-case sleeps double). A custom
// DelayFunc computes the exponential window and draws uniformly inside it.
func Retry(maxAttempts int, baseDelay time.Duration) retrypolicy.RetryPolicy[any] {
	maxDelay := baseDelay * 32
	return retrypolicy.NewBuilder[any]().
		WithMaxAttempts(maxAttempts).
		WithDelayFunc(func(exec failsafe.ExecutionAttempt[any]) time.Duration {
			return fullJitterDelay(baseDelay, maxDelay, exec.Retries())
		}).
		Build()
}

// fullJitterDelay draws a delay uniformly from [0, min(maxDelay, base·2^retries)].
func fullJitterDelay(base, maxDelay time.Duration, retries int) time.Duration {
	backoff := maxDelay
	// Guard the shift: retries ≥ 63 (or overflow past maxDelay) clamps to maxDelay.
	if retries < 63 {
		if d := base << uint(retries); d > 0 && d < maxDelay {
			backoff = d
		}
	}
	if backoff <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(backoff) + 1)) //nolint:gosec // jitter needs speed, not cryptographic randomness
}

// CircuitBreaker returns a CircuitBreaker[any] that opens after failureThreshold consecutive
// failures and transitions to half-open after openDelay.
func CircuitBreaker(failureThreshold uint, openDelay time.Duration) circuitbreaker.CircuitBreaker[any] {
	return circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(failureThreshold).
		WithDelay(openDelay).
		Build()
}

// Timeout returns a Timeout[any] that cancels execution after d.
func Timeout(d time.Duration) timeout.Timeout[any] {
	return timeout.New[any](d)
}

// Bulkhead returns a Bulkhead[any] that limits concurrent executions to maxConcurrent.
// Executions that would exceed the limit fail immediately with bulkhead.ErrFull.
func Bulkhead(maxConcurrent uint) bulkhead.Bulkhead[any] {
	return bulkhead.New[any](maxConcurrent)
}

// Do runs fn under the composed policies (outer-to-inner in the order given).
// fn receives the per-execution context which honours cancellation from both the
// caller's ctx and any Timeout policy in the chain.
//
// Example:
//
//	err := resilience.Do(ctx, fn, Retry(3, time.Second), CircuitBreaker(5, 30*time.Second))
func Do(ctx context.Context, fn func(context.Context) error, policies ...failsafe.Policy[any]) error {
	exec := failsafe.With[any](policies...).WithContext(ctx)
	return exec.RunWithExecution(func(e failsafe.Execution[any]) error {
		return fn(e.Context())
	})
}

// Get runs fn under the composed policies and returns a typed result.
//
// Because failsafe executors are parameterised on a single result type, Get uses
// Executor[any] internally and type-asserts the returned value to T. If fn returns
// a non-nil error the zero value of T is returned alongside the error.
// fn receives the per-execution context which honours cancellation from both the
// caller's ctx and any Timeout policy in the chain.
//
// Example:
//
//	val, err := resilience.Get[int](ctx, func(ctx context.Context) (int, error) {
//	    return 42, nil
//	}, Retry(3, time.Second))
func Get[T any](ctx context.Context, fn func(context.Context) (T, error), policies ...failsafe.Policy[any]) (T, error) {
	exec := failsafe.With[any](policies...).WithContext(ctx)
	result, err := exec.GetWithExecution(func(e failsafe.Execution[any]) (any, error) {
		return fn(e.Context())
	})
	if err != nil {
		var zero T
		return zero, err
	}
	// result may be nil if T is a pointer/interface type and fn returned (nil, nil).
	if result == nil {
		var zero T
		return zero, nil
	}
	return result.(T), nil //nolint:forcetypeassert // guaranteed by fn's return type
}

// NOTE: the former RateLimiter wrapper (golang.org/x/time/rate) was removed.
// It had no consumers and was shadowed by platform/web/ratelimit, which is the
// canonical per-key limiter (in-memory token bucket + distributed Redis Lua
// bucket) with Result metadata for 429 headers. Use that package instead.
