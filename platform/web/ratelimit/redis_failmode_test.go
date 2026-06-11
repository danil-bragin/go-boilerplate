package ratelimit

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrResult_FailClosed: a Redis error with fail-closed selected denies the
// request AND propagates the error (so the middleware answers 503, not a 429
// with a fabricated Retry-After). onError fires regardless. This is the C2
// fast-path proof without a Redis container.
func TestErrResult_FailClosed(t *testing.T) {
	t.Parallel()

	var seen error
	r := NewRedis(
		nil, 1, 5,
		WithFailClosed(true),
		WithOnError(func(err error) { seen = err }),
	)

	boom := errors.New("redis: dial tcp: connection refused")
	res, err := r.errResult(boom)

	assert.False(t, res.Allowed, "fail-closed must deny on Redis error")
	require.Error(t, err, "fail-closed must propagate the error")
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, boom, seen, "onError must observe the Redis error")
}

// TestErrResult_FailOpen: the default (fail-open) allows the request with an
// unknown budget and swallows the error, but still calls onError so the
// ratelimit.errors counter moves even when the request is admitted.
func TestErrResult_FailOpen(t *testing.T) {
	t.Parallel()

	var calls int
	r := NewRedis(
		nil, 1, 5,
		WithFailClosed(false),
		WithOnError(func(error) { calls++ }),
	)

	res, err := r.errResult(errors.New("redis down"))

	assert.True(t, res.Allowed, "fail-open must admit on Redis error")
	assert.NoError(t, err, "fail-open must not propagate the error")
	assert.EqualValues(t, -1, res.Remaining, "unknown budget sentinel")
	assert.Equal(t, 1, calls, "onError must fire even on a fail-open admit (counter moves)")
}
