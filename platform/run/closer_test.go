package run_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/run"
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
