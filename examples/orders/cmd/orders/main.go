// Command orders is the orders service entry point.
// It wires all components via orders.NewApp and blocks until a signal.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go-boilerplate/examples/orders"
	"go-boilerplate/platform/run"

	// automaxprocs sets GOMAXPROCS to match the container CPU quota at startup.
	// Go 1.25+ also does this natively when GOMAXPROCS is unset, but automaxprocs
	// is the belt-and-suspenders standard and works across all supported versions.
	_ "go.uber.org/automaxprocs"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	a, err := orders.NewApp(ctx)
	if err != nil {
		cancel()
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	a.Start()

	// run.Run blocks until SIGINT/SIGTERM, then calls Close on the registered closer.
	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.Closer()); err != nil {
		cancel()
		slog.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = a.Stop(shutdownCtx) // cancel consumer goroutines; closer already ran above
	slog.Info("shutdown complete")
}
