package audit

import (
	"sync"
	"time"
)

// denialBucketRate is the sustained refill rate (tokens/sec) of each
// per-(actor,action) denial audit bucket, and denialBucketBurst its depth.
//
// Rationale (security review #5): a denial audit is written OUT OF BAND via
// RecordOutOfBand, which takes the GLOBAL audit-chain FOR UPDATE lock. An
// authenticated attacker hammering a forbidden endpoint would otherwise turn
// every 403 into a serialized chain write, throttling all audit throughput —
// an authenticated DoS on the audit subsystem. Bounding denial writes per
// principal+action keeps legitimate denials auditable (the first burst is
// recorded) while coalescing a storm down to a trickle.
//
// 1 token/sec with a burst of 5 records the onset of an abuse pattern promptly
// and then samples it at ~1/s — enough for forensics, far below the rate that
// would saturate the chain lock.
const (
	denialBucketRate  = 1.0
	denialBucketBurst = 5.0
)

// denialLimiter is a tiny per-key token-bucket coalescer for denial audit
// writes. It is safe for concurrent use. Keys are (actor, action) pairs; an
// idle bucket is reclaimed by sweep so a churn of distinct actors cannot grow
// the map without bound.
type denialLimiter struct {
	mu      sync.Mutex
	buckets map[string]*denialBucket
	rate    float64
	burst   float64
	now     func() time.Time // injectable clock (tests)
}

type denialBucket struct {
	tokens float64
	last   time.Time
}

func newDenialLimiter() *denialLimiter {
	return &denialLimiter{
		buckets: make(map[string]*denialBucket),
		rate:    denialBucketRate,
		burst:   denialBucketBurst,
		now:     time.Now,
	}
}

// allow reports whether a denial audit for key may be WRITTEN now (true) or
// should be coalesced/dropped (false). It refills the bucket by elapsed time
// and consumes one token on admit.
func (l *denialLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b := l.buckets[key]
	if b == nil {
		// New key: full bucket, consume one.
		l.buckets[key] = &denialBucket{tokens: l.burst - 1, last: now}
		l.sweepLocked(now)
		return true
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = minFloat(b.tokens+elapsed*l.rate, l.burst)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// sweepLocked evicts buckets that have been idle long enough to have fully
// refilled (no admission state worth keeping). Called opportunistically on new
// key creation so the map stays bounded under actor churn. Caller holds mu.
func (l *denialLimiter) sweepLocked(now time.Time) {
	if len(l.buckets) < 1024 {
		return
	}
	fullRefill := l.burst / l.rate
	for k, b := range l.buckets {
		if now.Sub(b.last).Seconds() >= fullRefill {
			delete(l.buckets, k)
		}
	}
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
