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
	"io"
	"log/slog"
	"os"
	"time"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/examples/payments/internal/migrations"
	"go-boilerplate/examples/payments/internal/transport"
	"go-boilerplate/platform/audit"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/outboxkafka"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"
)

// Config aggregates all configuration for the payments service.
type Config struct {
	Log         log.Config
	Telemetry   telemetry.Config
	PG          pg.Config
	Kafka       kafka.Config
	EventsTopic string `env:"ORDERS_EVENTS_TOPIC" envDefault:"orders.events"`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// WithLogWriter overrides the log output writer (default: os.Stdout).
func WithLogWriter(w io.Writer) Option {
	return func(a *App) {
		a.logWriter = w
	}
}

// App holds all wired components for the payments service.
type App struct {
	cfg             Config
	logger          *slog.Logger
	closer          *run.Closer
	consumer        *kafka.Consumer
	relay           *outbox.Relay
	eventHandler    kafka.HandlerFunc
	cancelConsumers context.CancelFunc
	logWriter       io.Writer
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin consuming and relaying, and [App.Stop] to shut
// down gracefully.
func NewApp(ctx context.Context, opts ...Option) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	a := &App{
		cfg:       cfg,
		logWriter: os.Stdout,
	}

	for _, o := range opts {
		o(a)
	}

	logger, logSync := log.New(cfg.Log, a.logWriter)
	a.logger = logger
	slog.SetDefault(logger)

	closer := run.NewCloser()
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

	// Postgres pool.
	pool, err := pg.New(ctx, cfg.PG)
	if err != nil {
		return nil, err
	}
	closer.Add("pg", func(ctx context.Context) error {
		return pool.Close(ctx)
	})

	// Run migrations at startup.
	if err := pg.Migrate(ctx, cfg.PG.DSN, migrations.FS, "sql"); err != nil {
		return nil, err
	}

	// Kafka client + producer.
	producerCfg := cfg.Kafka
	producerCfg.GroupID = ""
	kafkaClient, err := kafka.NewClient(producerCfg)
	if err != nil {
		return nil, err
	}
	producer := kafka.NewProducer(kafkaClient)
	closer.Add("kafka-producer", func(ctx context.Context) error {
		return producer.Close(ctx)
	})

	// Ensure topics exist.
	if err := kafka.EnsureTopics(ctx, kafkaClient, 1, 1,
		cfg.EventsTopic,
		"payments.events",
	); err != nil {
		return nil, err
	}

	// Outbox repository + publisher + relay.
	outboxRepo := outbox.NewRepository(pool)
	publisher := outboxkafka.New(producer)
	relay := outbox.NewRelay(pool, publisher, outbox.RelayConfig{
		PollInterval: 200 * time.Millisecond,
	})
	relay.SetOnError(func(err error) {
		logger.Error("outbox relay error", "error", err)
	})
	a.relay = relay

	// Audit store.
	auditStore := audit.NewPgStore(pool)

	// Build and decorate the ProcessPayment handler.
	rawHandler := app.ProcessPaymentHandler(pool, outboxRepo)
	decoratedHandler := app.DecorateProcessPaymentHandler(rawHandler, auditStore)

	// Kafka consumer.
	consumerCfg := cfg.Kafka
	consumerCfg.GroupID = "payments"
	consumer, err := kafka.NewConsumer(consumerCfg, cfg.EventsTopic)
	if err != nil {
		return nil, err
	}
	closer.Add("kafka-consumer", func(context.Context) error {
		consumer.Close()
		return nil
	})
	a.consumer = consumer

	// Wire the Kafka handler.
	a.eventHandler = transport.NewEventHandler(pool, decoratedHandler)

	return a, nil
}

// Start launches background goroutines (outbox relay + Kafka consumer).
// Non-blocking.
func (a *App) Start() {
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancelConsumers = cancel

	go func() {
		if err := a.relay.Run(runCtx); err != nil && runCtx.Err() == nil {
			a.logger.Error("relay stopped unexpectedly", "error", err)
		}
	}()

	go func() {
		if err := a.consumer.Run(runCtx, a.eventHandler); err != nil && runCtx.Err() == nil {
			a.logger.Error("consumer stopped unexpectedly", "error", err)
		}
	}()

	a.logger.Info("payments service started")
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
