package run

import (
	"context"
	"os/signal"
	"syscall"
	"time"
)

// Options configures Run.
type Options struct {
	ShutdownTimeout time.Duration // max time for Closer to finish
}

// Run blocks until ctx is canceled or an OS termination signal (SIGINT/SIGTERM)
// arrives, then closes resources via closer within ShutdownTimeout.
func Run(ctx context.Context, opts Options, closer *Closer) error {
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 15 * time.Second
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	stop() // restore default signal handling so a second signal force-quits

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	return closer.Close(shutdownCtx)
}
