// Command migrate applies a service's embedded goose migrations to the
// database configured via PG_DSN (or PG_MIGRATE_URL — required behind
// PgBouncer transaction pooling; see platform/storage/pg.Migrate).
//
// This is the production migrate-job entrypoint: run it as a pre-deploy step
// (Kubernetes Job, CI stage) and set MIGRATE_ON_START=false on the app
// replicas so rollouts never race a long migration.
//
// Usage:
//
//	go run ./cmd/migrate -service orders        # one service
//	go run ./cmd/migrate -service all           # every example service
//	just migrate orders                         # justfile shorthand
//
// The same advisory lock as servicekit startup serializes concurrent runs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"syscall"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"

	"go-boilerplate/examples/gateway"
	"go-boilerplate/examples/notifications"
	"go-boilerplate/examples/orders"
	"go-boilerplate/examples/payments"
)

// services maps the -service flag to each service's embedded migration FS.
// All four embed their goose files under "sql" (internal/migrations).
var services = map[string]fs.FS{
	"gateway":       gateway.Migrations,
	"orders":        orders.Migrations,
	"payments":      payments.Migrations,
	"notifications": notifications.Migrations,
}

func main() {
	service := flag.String("service", "", "service whose migrations to apply: gateway|orders|payments|notifications|all")
	flag.Parse()

	if err := run(*service); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(service string) error {
	if service == "" {
		return errors.New("-service is required (gateway|orders|payments|notifications|all)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[pg.Config]()
	if err != nil {
		return err
	}

	targets := []string{service}
	if service == "all" {
		// Deterministic order; each service typically points PG_DSN at its own
		// database, so "all" is mainly useful for the single-instance dev setup.
		targets = []string{"gateway", "orders", "payments", "notifications"}
	}

	for _, name := range targets {
		fsys, ok := services[name]
		if !ok {
			return fmt.Errorf("unknown service %q (want gateway|orders|payments|notifications|all)", name)
		}
		if err := pg.Migrate(ctx, cfg.MigrateDSN().Reveal(), fsys, "sql"); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Printf("migrate: %s migrations applied\n", name)
	}
	return nil
}
