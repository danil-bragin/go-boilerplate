// Command projection runs the gateway read-model projection as a standalone
// consumer-only service (no public HTTP — admin server only). Deploy it with
// GATEWAY_EMBEDDED_PROJECTION=false on the gateway to split the edge from the
// read-model builder; both modes share the "gateway-projection" consumer
// group and inbox, so the handover is safe.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go-boilerplate/examples/gateway"
	"go-boilerplate/platform/run"

	// automaxprocs sets GOMAXPROCS to match the container CPU quota at startup.
	_ "go.uber.org/automaxprocs"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	a, err := gateway.NewProjectionApp(ctx)
	if err != nil {
		cancel()
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	a.Start()

	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.Closer()); err != nil {
		cancel()
		slog.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = a.Stop(shutdownCtx)
	slog.Info("shutdown complete")
}
