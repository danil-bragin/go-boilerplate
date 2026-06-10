package servicekit

// White-box unit tests for the AddPeriodicWorker jitter helper. Run in
// -short mode (no containers).

import (
	"testing"
	"time"
)

// TestJitterDelay_Bounds: jitterDelay must return a value in [0, jitter) for
// a positive jitter and exactly 0 for zero/negative jitter.
func TestJitterDelay_Bounds(t *testing.T) {
	const jitter = 100 * time.Millisecond
	for range 1000 {
		d := jitterDelay(jitter)
		if d < 0 || d >= jitter {
			t.Fatalf("jitterDelay(%v) = %v, want in [0, %v)", jitter, d, jitter)
		}
	}
	if d := jitterDelay(0); d != 0 {
		t.Fatalf("jitterDelay(0) = %v, want 0", d)
	}
	if d := jitterDelay(-time.Second); d != 0 {
		t.Fatalf("jitterDelay(-1s) = %v, want 0", d)
	}
}

// TestJitterDelay_NotConstant: over many samples the jitter must actually
// vary (a constant 0 would silently disable the thundering-herd spread).
func TestJitterDelay_NotConstant(t *testing.T) {
	const jitter = time.Hour
	first := jitterDelay(jitter)
	for range 100 {
		if jitterDelay(jitter) != first {
			return // saw variation
		}
	}
	t.Fatalf("jitterDelay(%v) returned the same value 100 times (%v)", jitter, first)
}
