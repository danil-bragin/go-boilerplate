package run_test

import (
	"context"
	"errors"
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
