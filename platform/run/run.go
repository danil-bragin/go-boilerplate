package run

import (
	"context"
	"os"
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
//
// If closer is nil, a no-op Closer is used so Run never panics.
//
// A second SIGINT/SIGTERM received while Close is running causes an immediate
// os.Exit(1) to prevent indefinite hang. This hard-exit path is not unit-tested
// because it calls os.Exit; the goroutine is made leak-safe by selecting on a
// done channel that is closed when Run returns normally.
func Run(ctx context.Context, opts Options, closer *Closer) error {
	if closer == nil {
		closer = NewCloser()
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 15 * time.Second
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	stop() // restore default signal handling

	// FIX 4: A second signal during shutdown must force-quit, even if Close hangs.
	// This goroutine is leak-safe: it exits when Run returns via the done channel.
	hardQuit := make(chan os.Signal, 1)
	signal.Notify(hardQuit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(hardQuit)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-hardQuit:
			os.Exit(1)
		case <-done:
		}
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()

	// FIX 5 assessment: returning ctx.Err() or context.Cause(ctx) when Close
	// succeeds would break TestRun_ReturnsWhenContextCanceled (which asserts
	// NoError on a context.WithCancel cancellation). Keeping nil on the happy
	// path is correct; only surface closeErr when Close itself fails.
	return closer.Close(shutdownCtx)
}
