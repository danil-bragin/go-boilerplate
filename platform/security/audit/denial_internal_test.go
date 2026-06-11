package audit

import (
	"testing"
	"time"
)

// TestDenialLimiter_BurstThenCoalesce: a single (actor,action) key admits the
// burst (5) then coalesces further writes until tokens refill. This is the
// per-principal bound that stops a denial flood from serializing on the chain
// lock (security review #5). The clock is injected so the test is deterministic.
func TestDenialLimiter_BurstThenCoalesce(t *testing.T) {
	l := newDenialLimiter()
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }

	const key = "actor-1\x00authz.denied"

	// Burst of 5 is admitted.
	admitted := 0
	for range 100 {
		if l.allow(key) {
			admitted++
		}
	}
	if admitted != int(denialBucketBurst) {
		t.Fatalf("burst: admitted=%d, want %d (bucket depth)", admitted, int(denialBucketBurst))
	}

	// Without time passing, everything else is coalesced.
	if l.allow(key) {
		t.Fatal("post-burst write must be coalesced until tokens refill")
	}

	// Advance 2s → 2 tokens refill (rate=1/s) → exactly 2 more admitted.
	now = now.Add(2 * time.Second)
	admitted = 0
	for range 100 {
		if l.allow(key) {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("after 2s refill: admitted=%d, want 2", admitted)
	}
}

// TestDenialLimiter_PerKeyIsolation: distinct (actor,action) keys have
// independent budgets — one principal's flood does not silence another's
// legitimate denials.
func TestDenialLimiter_PerKeyIsolation(t *testing.T) {
	l := newDenialLimiter()
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }

	// Exhaust actor-A.
	for range int(denialBucketBurst) + 3 {
		l.allow("actor-A\x00authz.denied")
	}
	if l.allow("actor-A\x00authz.denied") {
		t.Fatal("actor-A must be coalesced after its burst")
	}
	// actor-B is unaffected.
	if !l.allow("actor-B\x00authz.denied") {
		t.Fatal("actor-B must still be admitted (independent budget)")
	}
}
