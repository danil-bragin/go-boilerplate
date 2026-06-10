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

// TestMemory_ResetReported verifies the memory limiter reports the time until
// the bucket refills to full capacity (the RateLimit-Reset delta).
func TestMemory_ResetReported(t *testing.T) {
	clock := newFakeClock(time.Now())
	m := NewMemory(1, 2, WithClock(clock.Now)) // 1 rps, burst 2
	t.Cleanup(m.Close)
	ctx := context.Background()

	// First allow consumes one token: 1 token deficit at 1 rps → reset ≈ 1s.
	res, err := m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.Greater(t, res.Reset, time.Duration(0), "partial bucket must report a reset delta")
	assert.LessOrEqual(t, res.Reset, time.Second+50*time.Millisecond)

	// Empty bucket: 2 token deficit → reset ≈ 2s.
	res, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.Greater(t, res.Reset, time.Second)
	assert.LessOrEqual(t, res.Reset, 2*time.Second+50*time.Millisecond)
}
