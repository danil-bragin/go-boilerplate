// Package notifications implements the notifications service: a terminal event
// consumer that receives PaymentProcessed events from Kafka, deduplicates them
// via the inbox pattern, and invokes a Notifier for each unique payment.
//
// # Testability
//
// The App struct accepts functional options. Use [WithNotifier] to inject a
// capturing notifier in tests instead of the default structured-log call:
//
//	invocations := make(chan [3]string, 10)
//	app, err := notifications.NewApp(ctx,
//	    notifications.WithNotifier(func(orderID, paymentID, status string) {
//	        invocations <- [3]string{orderID, paymentID, status}
//	    }),
//	)
package notifications

import (
	"context"
	"io"

	"go-boilerplate/examples/notifications/internal/migrations"
	"go-boilerplate/examples/notifications/internal/transport"
	"go-boilerplate/examples/servicekit"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/run"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Config aggregates all configuration for the notifications service.
type Config struct {
	servicekit.Config
	PaymentsEventsTopic string `env:"PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
}

// notifOptions holds mutable options resolved before wiring.
type notifOptions struct {
	notifier transport.Notifier
}

// Option is a functional option for [NewApp].
type Option func(*notifOptions)

// WithNotifier overrides the default log-based notifier with a custom
// implementation. Tests inject a capturing function here to assert that the
// correct notifications are fired (and fired exactly once, thanks to inbox dedup).
func WithNotifier(n transport.Notifier) Option {
	return func(o *notifOptions) {
		o.notifier = n
	}
}

// WithLogWriter overrides the log output writer (default: os.Stdout).
// Kept for API compatibility; harness writes to os.Stdout.
func WithLogWriter(_ io.Writer) Option {
	return func(_ *notifOptions) {}
}

// App holds all wired components for the notifications service.
type App struct {
	svc *servicekit.Service
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin consuming events, and [App.Stop] to shut down
// gracefully.
func NewApp(ctx context.Context, opts ...Option) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	nOpts := &notifOptions{}
	for _, o := range opts {
		o(nOpts)
	}

	svc, err := servicekit.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}

	// Ensure the input topic exists; DLT topic handled by AddConsumer.
	if err := svc.EnsureTopics(ctx, cfg.PaymentsEventsTopic); err != nil {
		return nil, err
	}

	// Schema Registry (no-op when SERDE_SR_URL is unset).
	if err := svc.RegisterSchema(ctx, cfg.PaymentsEventsTopic, transport.PaymentProcessedEventType, &ordersv1.PaymentProcessed{}); err != nil {
		return nil, err
	}

	// Default notifier: structured log line. Tests override via WithNotifier.
	notifier := nOpts.notifier
	if notifier == nil {
		notifier = func(orderID, paymentID, status string) {
			svc.Logger().Info(
				"notification sent",
				"order_id", orderID,
				"payment_id", paymentID,
				"status", status,
			)
		}
	}

	var consumeOpts []consume.Option
	if sd := svc.Serde(); sd != nil {
		consumeOpts = append(consumeOpts, consume.WithSerde(sd))
	}
	evtHandler := transport.NewEventHandler(svc.Pool(), notifier, consumeOpts...)

	// Register consumer; harness wraps with WithRetry+DLT automatically.
	if err := svc.AddConsumer(ctx, "notifications", []string{cfg.PaymentsEventsTopic}, evtHandler); err != nil {
		return nil, err
	}

	return &App{svc: svc}, nil
}

// Start launches the Kafka consumer goroutine. Non-blocking.
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
