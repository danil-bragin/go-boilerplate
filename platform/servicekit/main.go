package servicekit

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go-boilerplate/platform/run"

	// automaxprocs sets GOMAXPROCS to match the container CPU quota at
	// startup. Go 1.25+ also does this natively when GOMAXPROCS is unset, but
	// automaxprocs is the belt-and-suspenders standard and works across all
	// supported versions. The blank import lives HERE (not in each main.go)
	// because Main is the single process entry point every service binary
	// goes through — one import, every binary correctly sized.
	_ "go.uber.org/automaxprocs"
)

// App is the minimal lifecycle contract [Main] drives. *Service satisfies it
// directly; the example services satisfy it with thin App wrappers around a
// *Service.
//
// Teardown happens exclusively through Closer (run.Run closes it on signal):
// there is intentionally NO Stop in this contract, because calling Stop after
// run.Run already ran the closer was the double-teardown bug the old
// hand-rolled mains all carried.
type App interface {
	// Start launches the app (non-blocking). An error is fatal: Main closes
	// whatever was already built and exits non-zero.
	Start() error
	// Closer returns the app's run.Closer; run.Run closes it (LIFO) on
	// SIGINT/SIGTERM within the shutdown timeout.
	Closer() *run.Closer
}

// mainShutdownTimeout bounds the closer run on shutdown — generous enough
// for DRAIN_GRACE plus consumer drain, hard enough that a stuck pod dies.
const mainShutdownTimeout = 15 * time.Second

// Main is the shared service entry point: build the app, start it, block
// until SIGINT/SIGTERM, then tear down via the app's Closer. A service main
// collapses to:
//
//	func main() {
//		servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
//			return orders.NewApp(ctx)
//		})
//	}
//
// Exit codes: 0 clean shutdown; 1 build failure, start failure, or teardown
// that finished with errors.
func Main(build func(ctx context.Context) (App, error)) {
	if code := runMain(build); code != 0 {
		os.Exit(code)
	}
}

// runMain is Main without the os.Exit, so tests can assert exit codes.
func runMain(build func(ctx context.Context) (App, error)) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := build(ctx)
	if err != nil {
		slog.Error("startup failed", "error", err)
		return 1
	}

	if err := a.Start(); err != nil {
		slog.Error("start failed", "error", err)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), mainShutdownTimeout)
		defer closeCancel()
		if closeErr := a.Closer().Close(closeCtx); closeErr != nil {
			slog.Error("teardown after failed start completed with errors", "error", closeErr)
		}
		return 1
	}

	// run.Run blocks until SIGINT/SIGTERM (or ctx cancellation), then closes
	// the app's closer LIFO within the shutdown timeout. The closer is the
	// ONLY teardown path — no Stop call afterwards.
	if err := run.Run(ctx, run.Options{ShutdownTimeout: mainShutdownTimeout}, a.Closer()); err != nil {
		slog.Error("shutdown completed with errors", "error", err)
		return 1
	}

	slog.Info("shutdown complete")
	return 0
}
