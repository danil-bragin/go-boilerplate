package run_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/run"
)

func TestRun_ReturnsWhenContextCanceled(t *testing.T) {
	c := run.NewCloser()
	var closed atomic.Bool
	c.Add("res", func(context.Context) error { closed.Store(true); return nil })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := run.Run(ctx, run.Options{ShutdownTimeout: time.Second}, c)
	require.NoError(t, err)
	require.True(t, closed.Load(), "closer must run on shutdown")
}
