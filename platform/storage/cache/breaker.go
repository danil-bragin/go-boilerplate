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

// newL2ErrorCounter builds the "cache.l2.errors" counter recorded by l2Error
// for every genuine L2 failure ({op=get|set|del}). Nil on creation failure —
// recording degrades to a no-op while the WARN log path stays alive.
func newL2ErrorCounter() metric.Int64Counter {
	ctr, err := otel.Meter("cache").Int64Counter(
		"cache.l2.errors",
		metric.WithDescription("L2 (Redis) operation errors, by op (get|set|del)"),
	)
	if err != nil {
		return nil
	}
	return ctr
}

// l2Allowed reports whether L2 may be used right now. False means the breaker
// is open (callers must degrade to L1-only behaviour without touching Redis)
// or the caller's context is already done — a cancelled caller can never
// complete an L2 op, so it must neither consume a half-open probe permit nor
// be allowed to feed a caller-driven failure into the breaker.
func (c *Cache) l2Allowed(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
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
//
// Failure ATTRIBUTION matters: an op that failed because the CALLER's context
// ended (cancellation or the caller's own deadline) says nothing about Redis
// health — recording it would let a burst of cancelled requests open the
// breaker against a healthy Redis. Such outcomes are recorded as neither
// success nor failure. An opCtx timeout (the L2OpTimeout bound from l2ctx)
// with a LIVE caller ctx is a genuine L2 failure and still counts.
//
// Half-open exception: a probe permit acquired by l2Allowed is only released
// by recording a result, so during half-open a caller-cancelled probe is
// conservatively recorded as a failure (breaker re-opens, retries after the
// delay) — skipping the record would leak the probe permit and wedge the
// breaker half-open forever.
func (c *Cache) l2Done(ctx context.Context, err error) {
	switch {
	case err == nil || rueidis.IsRedisNil(err):
		c.l2cb.RecordSuccess()
	case ctx.Err() == nil || c.l2cb.State() == circuitbreaker.HalfOpenState:
		c.l2cb.RecordFailure()
	default:
		// Caller-driven cancellation outside half-open: record nothing.
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
