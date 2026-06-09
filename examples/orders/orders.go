// Package orders implements the orders service: consumes CreateOrderCommand
// from Kafka, writes orders to Postgres, and emits OrderCreated events via
// the transactional outbox.
//
// # Testability
//
// The App struct is constructed via [NewApp] which reads configuration from
// environment variables. In tests, set the relevant env vars with t.Setenv
// before calling NewApp:
//
//	t.Setenv("PG_DSN", dsn)
//	t.Setenv("KAFKA_BROKERS", broker)
//	app, err := orders.NewApp(ctx)
package orders

import (
	"context"
	"io"
	"time"

	"go-boilerplate/examples/internal/service"
	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/examples/orders/internal/migrations"
	"go-boilerplate/examples/orders/internal/transport"
	"go-boilerplate/platform/audit"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/run"
)

// Config aggregates all configuration for the orders service.
type Config struct {
	service.Config
	CommandsTopic string `env:"ORDERS_COMMANDS_TOPIC" envDefault:"orders.commands"`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// WithLogWriter overrides the log output writer (default: os.Stdout).
// NOTE: The harness always writes to os.Stdout; this option is kept for
// API compatibility with tests and e2e. Pass io.Discard to suppress logs.
func WithLogWriter(_ io.Writer) Option {
	return func(_ *App) {}
}

// App holds all wired components for the orders service.
type App struct {
	svc *service.Service
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin consuming and relaying, and [App.Stop] to shut
// down gracefully.
func NewApp(ctx context.Context, opts ...Option) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	a := &App{}
	for _, o := range opts {
		o(a)
	}

	svc, err := service.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}
	a.svc = svc

	// Ensure topics (commands, events); DLT topics handled by AddConsumer.
	if err := svc.EnsureTopics(ctx, 1, 1, cfg.CommandsTopic, "orders.events"); err != nil {
		return nil, err
	}

	// Outbox relay + cleaner (publishes OrderCreated events).
	outboxRepo := outbox.NewRepository(svc.Pool())
	svc.AddOutboxRelay(svc.DefaultOutboxPublisher(), outbox.RelayConfig{
		PollInterval: 200 * time.Millisecond,
	})

	// Build the domain handler.
	auditStore := audit.NewPgStore(svc.Pool())
	rawHandler := app.CreateOrderHandler(svc.Pool(), outboxRepo)
	decoratedHandler := app.DecorateCreateOrderHandler(rawHandler, auditStore)
	cmdHandler := transport.NewCommandHandler(svc.Pool(), decoratedHandler)

	// Register consumer; harness wraps with WithRetry+DLT automatically.
	if err := svc.AddConsumer(ctx, "orders-consumer", []string{cfg.CommandsTopic}, cmdHandler); err != nil {
		return nil, err
	}

	// Launch audit_log cleanup; defaults to 90-day retention / 6-hour interval.
	svc.AddAuditCleanup(auditStore, cfg.AuditCleanupInterval, cfg.AuditRetention)

	return a, nil
}

// Start launches background goroutines (outbox relay + cleaner + Kafka consumer).
// Non-blocking.
func (a *App) Start() {
	if err := a.svc.Start(); err != nil {
		a.svc.Logger().Error("failed to start service", "error", err)
	}
}

// Stop cancels consumer goroutines and closes all resources.
func (a *App) Stop(ctx context.Context) error {
	return a.svc.Stop(ctx)
}

// Closer returns the run.Closer for integration with run.Run.
func (a *App) Closer() *run.Closer {
	return a.svc.Closer()
}
