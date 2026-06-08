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
	"log/slog"
	"os"

	"go-boilerplate/examples/notifications/internal/migrations"
	"go-boilerplate/examples/notifications/internal/transport"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"
)

// Config aggregates all configuration for the notifications service.
type Config struct {
	Log                 log.Config
	Telemetry           telemetry.Config
	PG                  pg.Config
	Kafka               kafka.Config
	PaymentsEventsTopic string `env:"PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// WithNotifier overrides the default log-based notifier with a custom
// implementation. Tests inject a capturing function here to assert that the
// correct notifications are fired (and fired exactly once, thanks to inbox dedup).
func WithNotifier(n transport.Notifier) Option {
	return func(a *App) {
		a.notifier = n
	}
}

// WithLogWriter overrides the log output writer (default: os.Stdout).
func WithLogWriter(w io.Writer) Option {
	return func(a *App) {
		a.logWriter = w
	}
}

// App holds all wired components for the notifications service.
type App struct {
	cfg             Config
	logger          *slog.Logger
	closer          *run.Closer
	consumer        *kafka.Consumer
	eventHandler    kafka.HandlerFunc
	cancelConsumers context.CancelFunc

	// notifier is invoked for each unique PaymentProcessed event after dedup.
	// Default: structured log line. Replaceable via WithNotifier for tests.
	notifier  transport.Notifier
	logWriter io.Writer
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin consuming events, and [App.Stop] (or the
// run.Closer registered inside) to shut down gracefully.
func NewApp(ctx context.Context, opts ...Option) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	a := &App{
		cfg:       cfg,
		logWriter: os.Stdout,
	}

	// Apply options before building components so overrides (logWriter, etc.)
	// are in effect before first use.
	for _, o := range opts {
		o(a)
	}

	logger, logSync := log.New(cfg.Log, a.logWriter)
	a.logger = logger
	slog.SetDefault(logger)

	closer := run.NewCloser()
	// log-sync runs last (reverse registration order) so shutdown logs flush.
	closer.Add("log-sync", func(context.Context) error {
		_ = logSync()
		return nil
	})
	a.closer = closer

	shutdownTel, err := telemetry.Setup(ctx, cfg.Telemetry)
	if err != nil {
		return nil, err
	}
	closer.Add("telemetry", func(ctx context.Context) error {
		return shutdownTel(ctx)
	})

	// Postgres pool — used only for the inbox table (no business tables).
	pool, err := pg.New(ctx, cfg.PG)
	if err != nil {
		return nil, err
	}
	closer.Add("pg", func(ctx context.Context) error {
		return pool.Close(ctx)
	})

	// Run migrations (creates the inbox table only).
	if err := pg.Migrate(ctx, cfg.PG.DSN, migrations.FS, "sql"); err != nil {
		return nil, err
	}

	// Kafka client — consumer only, no producer needed.
	consumerKafkaCfg := cfg.Kafka
	consumerKafkaCfg.GroupID = "notifications"

	kafkaClient, err := kafka.NewClient(consumerKafkaCfg)
	if err != nil {
		return nil, err
	}

	// Ensure the input topic exists before starting the consumer.
	if err := kafka.EnsureTopics(ctx, kafkaClient, 1, 1, cfg.PaymentsEventsTopic); err != nil {
		return nil, err
	}

	consumer, err := kafka.NewConsumer(consumerKafkaCfg, cfg.PaymentsEventsTopic)
	if err != nil {
		return nil, err
	}
	closer.Add("kafka-consumer", func(context.Context) error {
		consumer.Close()
		return nil
	})
	a.consumer = consumer

	// Default notifier: structured log line. Tests override via WithNotifier.
	if a.notifier == nil {
		a.notifier = func(orderID, paymentID, status string) {
			logger.Info("notification sent",
				"order_id", orderID,
				"payment_id", paymentID,
				"status", status,
			)
		}
	}

	a.eventHandler = transport.NewEventHandler(pool, a.notifier)

	return a, nil
}

// Start launches the Kafka consumer goroutine. Non-blocking.
func (a *App) Start() {
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancelConsumers = cancel

	go func() {
		if err := a.consumer.Run(runCtx, a.eventHandler); err != nil && runCtx.Err() == nil {
			a.logger.Error("consumer stopped unexpectedly", "error", err)
		}
	}()

	a.logger.Info("notifications service started")
}

// Stop cancels consumer goroutines and closes all resources.
func (a *App) Stop(ctx context.Context) error {
	if a.cancelConsumers != nil {
		a.cancelConsumers()
	}
	return a.closer.Close(ctx)
}

// Closer returns the run.Closer for integration with run.Run.
func (a *App) Closer() *run.Closer {
	return a.closer
}
