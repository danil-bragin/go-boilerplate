package cache

import (
	"context"
	"errors"
	"time"

	"go-boilerplate/platform/resilience"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// ErrL2Unavailable is returned by Delete when the L2 circuit breaker is open:
// the key was evicted from L1 and the eviction broadcast was skipped, but the
// authoritative Redis tier may still hold the value.
var ErrL2Unavailable = errors.New("cache: L2 unavailable (circuit breaker open)")

// L2 circuit breaker tuning: open after 5 consecutive failures, probe again
// (half-open) after 15s. While open every L2 op is skipped instantly, so a
// Redis outage costs an L1 lookup instead of a per-request connection wait.
const (
	l2BreakerFailureThreshold = 5
	l2BreakerOpenDelay        = 15 * time.Second
)

// newL2Breaker builds the breaker plus its state gauge
// ("cache.l2.breaker_state": 0=closed 1=open 2=half-open).
func newL2Breaker() (circuitbreaker.CircuitBreaker[any], metric.Int64Gauge) {
	cb := resilience.CircuitBreaker(l2BreakerFailureThreshold, l2BreakerOpenDelay)
	gauge, err := otel.Meter("cache").Int64Gauge(
		"cache.l2.breaker_state",
		metric.WithDescription("L2 (Redis) circuit breaker state: 0=closed 1=open 2=half-open"),
	)
	if err != nil {
		// Degrade gracefully: a nil gauge disables recording.
		return cb, nil
	}
	return cb, gauge
}

// l2Allowed reports whether L2 may be used right now. False means the breaker
// is open: callers must degrade to L1-only behaviour without touching Redis.
func (c *Cache) l2Allowed(ctx context.Context) bool {
	ok := c.l2cb.TryAcquirePermit()
	c.recordBreakerState(ctx)
	return ok
}

// l2ctx bounds an L2 operation with Config.L2OpTimeout. rueidis retries
// network errors until the ctx deadline, so the bound is what turns an
// outage into a recordable failure instead of an indefinite per-op hang.
func (c *Cache) l2ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := c.cfg.L2OpTimeout
	if timeout <= 0 {
		timeout = defaultL2OpTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// l2Done feeds an L2 operation outcome back into the breaker. A Redis nil
// reply is a healthy response, not a failure.
func (c *Cache) l2Done(ctx context.Context, err error) {
	if err == nil || rueidis.IsRedisNil(err) {
		c.l2cb.RecordSuccess()
	} else {
		c.l2cb.RecordFailure()
	}
	c.recordBreakerState(ctx)
}

func (c *Cache) recordBreakerState(ctx context.Context) {
	if c.l2cbGauge == nil {
		return
	}
	var state int64
	switch c.l2cb.State() {
	case circuitbreaker.ClosedState:
		state = 0
	case circuitbreaker.OpenState:
		state = 1
	case circuitbreaker.HalfOpenState:
		state = 2
	}
	c.l2cbGauge.Record(ctx, state)
}
