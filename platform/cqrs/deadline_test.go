package cqrs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-boilerplate/platform/cqrs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadline_SlowHandlerReturnsTypedError: a handler that outlives the
// deadline must surface a typed error that is errors.Is-able both as
// cqrs.ErrDeadlineExceeded and as context.DeadlineExceeded.
func TestDeadline_SlowHandlerReturnsTypedError(t *testing.T) {
	slow := func(ctx context.Context, _ string) (string, error) {
		select {
		case <-time.After(5 * time.Second):
			return "too late", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	h := cqrs.Decorate(slow, cqrs.Deadline[string, string](20*time.Millisecond))

	_, err := h(context.Background(), "cmd")
	require.Error(t, err)
	assert.ErrorIs(t, err, cqrs.ErrDeadlineExceeded, "error must match the cqrs sentinel")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "sentinel must wrap context.DeadlineExceeded")
}

// TestDeadline_FastHandlerUnaffected: a handler finishing within the deadline
// passes its result and error through untouched.
func TestDeadline_FastHandlerUnaffected(t *testing.T) {
	fast := func(ctx context.Context, in string) (string, error) {
		// The behavior must install a real deadline on ctx.
		dl, ok := ctx.Deadline()
		require.True(t, ok, "handler ctx must carry a deadline")
		require.WithinDuration(t, time.Now().Add(time.Minute), dl, time.Minute)
		return in + "-done", nil
	}

	h := cqrs.Decorate(fast, cqrs.Deadline[string, string](time.Minute))

	res, err := h(context.Background(), "cmd")
	require.NoError(t, err)
	assert.Equal(t, "cmd-done", res)
}

// TestDeadline_HandlerErrorNotMasked: a domain error from the handler must not
// be rewritten as a deadline error when the deadline has not fired.
func TestDeadline_HandlerErrorNotMasked(t *testing.T) {
	sentinel := errors.New("domain boom")
	failing := func(context.Context, string) (string, error) { return "", sentinel }

	h := cqrs.Decorate(failing, cqrs.Deadline[string, string](time.Minute))

	_, err := h(context.Background(), "cmd")
	require.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, cqrs.ErrDeadlineExceeded)
}

// TestDeadline_TighterCallerDeadlineWins: when the caller's ctx already has a
// shorter deadline, WithTimeout keeps the tighter one.
func TestDeadline_TighterCallerDeadlineWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	slow := func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}

	h := cqrs.Decorate(slow, cqrs.Deadline[string, string](time.Hour))

	start := time.Now()
	_, err := h(ctx, "cmd")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "caller's tighter deadline must apply")
}
