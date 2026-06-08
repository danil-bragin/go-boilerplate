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
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := orders.NewApp(ctx)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	a.Start()

	// run.Run blocks until SIGINT/SIGTERM, then calls Close on the registered closer.
	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.Closer()); err != nil {
		slog.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = a.Stop(shutdownCtx) // cancel consumer goroutines; closer already ran above
	slog.Info("shutdown complete")
}
