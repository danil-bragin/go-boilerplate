package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemory_ResultFields verifies that the memory limiter reports real
// Limit / Remaining / RetryAfter values, not just an allow/deny bit.
func TestMemory_ResultFields(t *testing.T) {
	clock := newFakeClock(time.Now())
	m := NewMemory(1, 2, WithClock(clock.Now)) // 1 rps, burst 2
	t.Cleanup(m.Close)
	ctx := context.Background()

	// First request: allowed, one token left.
	res, err := m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 2, res.Limit)
	assert.EqualValues(t, 1, res.Remaining)
	assert.Zero(t, res.RetryAfter, "allowed result must not carry RetryAfter")

	// Second request: allowed, bucket empty.
	res, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 0, res.Remaining)

	// Third request: denied with a real retry hint (≈1s until the next token
	// at 1 rps).
	res, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.False(t, res.Allowed)
	assert.EqualValues(t, 0, res.Remaining)
	assert.Greater(t, res.RetryAfter, time.Duration(0), "denied result must carry RetryAfter")
	assert.LessOrEqual(t, res.RetryAfter, time.Second+50*time.Millisecond)

	// After the refill interval the key admits again.
	clock.Advance(time.Second)
	res, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.True(t, res.Allowed)
}
