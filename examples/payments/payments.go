// Package payments implements the payments service: consumes OrderCreated
// events from Kafka, writes payments to Postgres, and emits PaymentProcessed
// events via the transactional outbox.
//
// # Testability
//
// The App struct is constructed via [NewApp] which reads configuration from
// environment variables. In tests, set the relevant env vars with t.Setenv
// before calling NewApp:
//
//	t.Setenv("PG_DSN", dsn)
//	t.Setenv("KAFKA_BROKERS", broker)
//	app, err := payments.NewApp(ctx)
package payments

import (
	"context"
	"time"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/examples/payments/internal/migrations"
	"go-boilerplate/examples/payments/internal/transport"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/servicekit"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Config aggregates all configuration for the payments service.
type Config struct {
	servicekit.Config
	EventsTopic string `env:"ORDERS_EVENTS_TOPIC" envDefault:"orders.events"`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// App holds all wired components for the payments service.
type App struct {
	svc *servicekit.Service
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

	svc, err := servicekit.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}
	a.svc = svc

	// Ensure source topic and output topic; DLT topics handled by AddConsumer.
	if err := svc.EnsureTopics(ctx, cfg.EventsTopic, "payments.events"); err != nil {
		return nil, err
	}

	// Schema Registry (no-op when SERDE_SR_URL is unset).
	if err := svc.RegisterSchema(ctx, cfg.EventsTopic, transport.OrderCreatedEventType, &ordersv1.OrderCreated{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, "payments.events", app.PaymentProcessedEventType, &ordersv1.PaymentProcessed{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, "payments.events", app.PaymentFailedEventType, &ordersv1.PaymentFailed{}); err != nil {
		return nil, err
	}

	// Outbox relay + cleaner (publishes PaymentProcessed events).
	outboxRepo := outbox.NewRepository(svc.Pool())
	if err := svc.AddOutboxRelay(svc.DefaultOutboxPublisher(), outbox.RelayConfig{
		PollInterval: 200 * time.Millisecond,
	}); err != nil {
		return nil, err
	}

	// Build the domain handler.
	auditStore := audit.NewPgStore(svc.Pool())
	rawHandler := app.ProcessPaymentHandler(svc.Pool(), outboxRepo)
	decoratedHandler := app.DecorateProcessPaymentHandler(rawHandler, auditStore)
	var consumeOpts []consume.Option
	if sd := svc.Serde(); sd != nil {
		consumeOpts = append(consumeOpts, consume.WithSerde(sd))
	}
	evtHandler := transport.NewEventHandler(svc.Pool(), decoratedHandler, consumeOpts...)

	// Register consumer; harness wraps with WithRetry+DLT automatically.
	if err := svc.AddConsumer(ctx, "payments", []string{cfg.EventsTopic}, evtHandler); err != nil {
		return nil, err
	}

	// Launch audit_log cleanup; defaults to 90-day retention / 6-hour interval.
	svc.AddAuditCleanup(auditStore, cfg.AuditCleanupInterval, cfg.AuditRetention)

	return a, nil
}

// Start launches background goroutines (outbox relay + cleaner + Kafka consumer).
// Non-blocking.
func (a *App) Start() error {
	return a.svc.Start()
}

// Stop cancels consumer goroutines and closes all resources.
func (a *App) Stop(ctx context.Context) error {
	return a.svc.Stop(ctx)
}

// Closer returns the run.Closer for integration with run.Run.
func (a *App) Closer() *run.Closer {
	return a.svc.Closer()
}
