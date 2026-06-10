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
	"time"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/examples/orders/internal/migrations"
	"go-boilerplate/examples/orders/internal/transport"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/retry"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/servicekit"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Config aggregates all configuration for the orders service.
type Config struct {
	servicekit.Config
	CommandsTopic string `env:"ORDERS_COMMANDS_TOPIC" envDefault:"orders.commands"`
	// PaymentsEventsTopic is consumed to record payment outcomes on order rows
	// (paid / payment_failed) so the unpaid watcher is a pure local query.
	PaymentsEventsTopic string `env:"PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
	// UnpaidCheckInterval is how often the unpaid-order watcher polls.
	UnpaidCheckInterval time.Duration `env:"ORDERS_UNPAID_CHECK_INTERVAL" envDefault:"1m"`
	// PaymentDeadline is how long an order may stay 'created' (unpaid) before
	// an OrderPaymentTimedOut event is emitted.
	PaymentDeadline time.Duration `env:"ORDERS_PAYMENT_DEADLINE" envDefault:"15m"`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// App holds all wired components for the orders service.
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

	// Ensure topics (commands, events); DLT topics handled by AddConsumer.
	if err := svc.EnsureTopics(ctx, cfg.CommandsTopic, order.EventsTopic, cfg.PaymentsEventsTopic); err != nil {
		return nil, err
	}

	// Schema Registry (no-op when SERDE_SR_URL is unset): register the schema
	// of every consumed and produced message type, fail-fast on registry errors.
	if err := svc.RegisterSchema(ctx, cfg.CommandsTopic, transport.CommandEventType, &ordersv1.CreateOrderCommand{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, order.EventsTopic, order.OrderCreatedEventType, &ordersv1.OrderCreated{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, order.EventsTopic, order.OrderPaymentTimedOutEventType, &ordersv1.OrderPaymentTimedOut{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.PaymentsEventsTopic, transport.PaymentProcessedEventType, &ordersv1.PaymentProcessed{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.PaymentsEventsTopic, transport.PaymentFailedEventType, &ordersv1.PaymentFailed{}); err != nil {
		return nil, err
	}

	// Outbox relay + cleaner (publishes OrderCreated events).
	outboxRepo := outbox.NewRepository(svc.Pool())
	if err := svc.AddOutboxRelay(svc.DefaultOutboxPublisher(), outbox.RelayConfig{
		PollInterval: 200 * time.Millisecond,
	}); err != nil {
		return nil, err
	}

	// Domain service: repository + outbox publisher share the ambient
	// transaction, so row writes and event enqueues commit together.
	repo := order.NewPgRepository(svc.Pool())
	domainSvc := order.NewService(repo, outboxRepo, svc.Logger(), cfg.PaymentDeadline)

	// Build the command handler (thin adapter over the domain service).
	auditStore := audit.NewPgStore(svc.Pool())
	rawHandler := app.CreateOrderHandler(domainSvc)
	decoratedHandler := app.DecorateCreateOrderHandler(rawHandler, auditStore)
	var consumeOpts []consume.Option
	if sd := svc.Serde(); sd != nil {
		consumeOpts = append(consumeOpts, consume.WithSerde(sd))
	}
	cmdHandler := transport.NewCommandHandler(svc.Pool(), decoratedHandler, consumeOpts...)

	// Register consumer with tiered retry-topic redrive (non-blocking redrive demo).
	if err := svc.AddConsumerWithRetry(ctx, "orders-consumer", []string{cfg.CommandsTopic}, cmdHandler, retry.DefaultPolicy()); err != nil {
		return nil, err
	}

	// Payment-outcome consumer: records paid/payment_failed on the local order
	// rows (inbox-deduped) so the unpaid watcher below never needs to look
	// beyond this service's own database.
	paymentsHandler := transport.NewPaymentsEventHandler(svc.Pool(), domainSvc, svc.Logger(), consumeOpts...)
	if err := svc.AddConsumer(ctx, "orders-payments", []string{cfg.PaymentsEventsTopic}, paymentsHandler); err != nil {
		return nil, err
	}

	// Unpaid-order watcher: emits OrderPaymentTimedOut (via outbox, exactly
	// once per order) for orders still 'created' past the payment deadline.
	// singleActive: only one instance scans at a time (advisory-lock leader);
	// the CAS guard inside the watcher keeps emission exactly-once even
	// during the brief leader-overlap window.
	watcher := app.NewUnpaidWatcher(svc.Pool(), domainSvc)
	if err := svc.AddPeriodicWorker("unpaid-watcher", cfg.UnpaidCheckInterval, 0, true, watcher.Poll); err != nil {
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

// Fatal exposes the harness fatal-error channel so servicekit.Main tears the
// process down (non-zero exit) when a public HTTP server dies after Start.
func (a *App) Fatal() <-chan error {
	return a.svc.Fatal()
}
