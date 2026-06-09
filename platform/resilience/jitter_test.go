package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullJitterDelay_Bounds: true full jitter draws each delay uniformly from
// [0, backoff] where backoff = min(maxDelay, baseDelay·2^retries). Across many
// samples every value must stay inside the window (the old WithJitterFactor(1.0)
// implementation produced values up to 2× the computed delay) and the samples
// must actually spread across the window (uniformity smoke check).
func TestFullJitterDelay_Bounds(t *testing.T) {
	const samples = 5000
	base := 100 * time.Millisecond
	maxDelay := base * 32

	for retries, backoff := range map[int]time.Duration{
		0: base,     // first retry: [0, base]
		1: base * 2, // second: [0, 2·base]
		3: base * 8, // fourth: [0, 8·base]
		9: maxDelay, // beyond the cap: [0, 32·base]
		// (2^9·base would be 51.2s — must be clamped to maxDelay)
	} {
		var lo, hi time.Duration = time.Hour, -1
		for range samples {
			d := fullJitterDelay(base, maxDelay, retries)
			require.GreaterOrEqual(t, d, time.Duration(0), "retries=%d: delay below 0", retries)
			require.LessOrEqual(t, d, backoff, "retries=%d: delay above backoff window", retries)
			if d < lo {
				lo = d
			}
			if d > hi {
				hi = d
			}
		}
		// Distribution smoke check: with 5000 uniform samples the observed min
		// must fall in the lowest quarter and the max in the highest quarter.
		assert.Less(t, lo, backoff/4, "retries=%d: min sample suspiciously high — not full jitter", retries)
		assert.Greater(t, hi, backoff*3/4, "retries=%d: max sample suspiciously low — not full jitter", retries)
	}
}

// TestRetry_DelaysWithinFullJitterWindow: end-to-end check that the built
// policy's actual sleep time stays within the cumulative full-jitter budget.
// 3 attempts → 2 delays drawn from [0, base] and [0, 2·base].
func TestRetry_DelaysWithinFullJitterWindow(t *testing.T) {
	const base = 50 * time.Millisecond
	errAlways := errors.New("always")
	start := time.Now()

	err := Do(t.Context(), func(context.Context) error { return errAlways }, Retry(3, base))
	elapsed := time.Since(start)

	require.Error(t, err)
	// Upper bound: base + 2·base (+ generous scheduling slack).
	assert.Less(t, elapsed, 3*base+200*time.Millisecond,
		"total retry time exceeds the full-jitter budget — jitter window too wide")
}
