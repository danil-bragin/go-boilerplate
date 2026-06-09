package run_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go-boilerplate/platform/run"

	"github.com/stretchr/testify/require"
)

func TestCloser_RunsInReverseOrder(t *testing.T) {
	var order []string
	c := run.NewCloser()
	c.Add("a", func(context.Context) error { order = append(order, "a"); return nil })
	c.Add("b", func(context.Context) error { order = append(order, "b"); return nil })
	c.Add("c", func(context.Context) error { order = append(order, "c"); return nil })

	require.NoError(t, c.Close(context.Background()))
	require.Equal(t, []string{"c", "b", "a"}, order)
}

func TestCloser_AggregatesErrorsButRunsAll(t *testing.T) {
	var ran int
	c := run.NewCloser()
	c.Add("ok1", func(context.Context) error { ran++; return nil })
	c.Add("bad", func(context.Context) error { ran++; return errors.New("boom") })
	c.Add("ok2", func(context.Context) error { ran++; return nil })

	err := c.Close(context.Background())
	require.Error(t, err)
	require.Equal(t, 3, ran) // all teardowns ran despite the error
}

// FIX 1: Add after Close must not silently drop the teardown.
func TestCloser_AddAfterCloseRunsImmediately(t *testing.T) {
	c := run.NewCloser()
	require.NoError(t, c.Close(context.Background()))

	var ran bool
	c.Add("late", func(context.Context) error {
		ran = true
		return nil
	})
	require.True(t, ran, "teardown added after Close must run immediately, not be dropped")
}

// FIX 2: Close is idempotent; the second call must be a no-op.
func TestCloser_DoubleCloseIsNoOp(t *testing.T) {
	var count int
	c := run.NewCloser()
	c.Add("once", func(context.Context) error { count++; return nil })

	require.NoError(t, c.Close(context.Background()))
	require.NoError(t, c.Close(context.Background()))
	require.Equal(t, 1, count, "teardown must run exactly once across two Close calls")
}

// TestCloser_LateAddErrorIsLogged: a teardown added after Close runs
// immediately AND its error must be logged (not silently swallowed) via the
// default slog logger.
func TestCloser_LateAddErrorIsLogged(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	c := run.NewCloser()
	require.NoError(t, c.Close(context.Background()))

	c.Add("late-broken", func(context.Context) error { return errors.New("late boom") })

	out := buf.String()
	require.Contains(t, out, "late-broken", "log line must name the resource")
	require.Contains(t, out, "late boom", "log line must carry the teardown error")
}

// TestCloser_CancelledCtxStillRunsAll: when the teardown ctx is already
// cancelled, every remaining teardown must still be attempted (best-effort
// cleanup) and the returned error must record the context error.
func TestCloser_CancelledCtxStillRunsAll(t *testing.T) {
	var ran []string
	c := run.NewCloser()
	c.Add("a", func(context.Context) error { ran = append(ran, "a"); return nil })
	c.Add("b", func(context.Context) error { ran = append(ran, "b"); return nil })
	c.Add("c", func(context.Context) error { ran = append(ran, "c"); return nil })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // budget exhausted before teardown even starts

	err := c.Close(ctx)
	require.Error(t, err, "Close must surface the exhausted teardown budget")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"c", "b", "a"}, ran, "all teardowns must still be attempted")
}

// TestCloser_CtxErrRecordedOnce: the context error is recorded once, not once
// per remaining item.
func TestCloser_CtxErrRecordedOnce(t *testing.T) {
	c := run.NewCloser()
	for _, name := range []string{"x", "y", "z"} {
		c.Add(name, func(context.Context) error { return nil })
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Close(ctx)
	require.Error(t, err)
	require.Equal(t, 1, strings.Count(err.Error(), context.Canceled.Error()),
		"ctx error must be recorded exactly once")
}
