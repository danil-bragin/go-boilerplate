// Command payments is the payments service: consumes OrderCreated events from Kafka,
// writes payments to Postgres, and emits PaymentProcessed events via the transactional outbox.
package main

import (
	"context"
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

// appConfig aggregates all configuration for the payments service.
type appConfig struct {
	Log         log.Config
	Telemetry   telemetry.Config
	PG          pg.Config
	Kafka       kafka.Config
	EventsTopic string `env:"ORDERS_EVENTS_TOPIC" envDefault:"orders.events"`
}

// paymentsApp holds all wired components.
type paymentsApp struct {
	cfg             appConfig
	logger          *slog.Logger
	closer          *run.Closer
	consumer        *kafka.Consumer
	relay           *outbox.Relay
	eventHandler    kafka.HandlerFunc
	cancelConsumers context.CancelFunc
}

func newApp(ctx context.Context) (*paymentsApp, error) {
	cfg, err := config.Load[appConfig]()
	if err != nil {
		return nil, err
	}

	logger, logSync := log.New(cfg.Log, os.Stdout)
	slog.SetDefault(logger)

	closer := run.NewCloser()
	// log-sync runs last (reverse order) so all shutdown logs are flushed.
	closer.Add("log-sync", func(context.Context) error {
		_ = logSync()
		return nil
	})

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

	// Kafka client + producer (producer uses a client without group settings).
	producerCfg := cfg.Kafka
	producerCfg.GroupID = "" // producer does not need a group
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

	// Wire the Kafka handler.
	eventHandler := transport.NewEventHandler(pool, decoratedHandler)

	return &paymentsApp{
		cfg:          cfg,
		logger:       logger,
		closer:       closer,
		consumer:     consumer,
		relay:        relay,
		eventHandler: eventHandler,
	}, nil
}

func (a *paymentsApp) start() {
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancelConsumers = cancel

	// Start outbox relay.
	go func() {
		if err := a.relay.Run(runCtx); err != nil && runCtx.Err() == nil {
			a.logger.Error("relay stopped unexpectedly", "error", err)
		}
	}()

	// Start Kafka consumer.
	go func() {
		if err := a.consumer.Run(runCtx, a.eventHandler); err != nil && runCtx.Err() == nil {
			a.logger.Error("consumer stopped unexpectedly", "error", err)
		}
	}()

	a.logger.Info("payments service started")
}

func (a *paymentsApp) stop(ctx context.Context) error {
	if a.cancelConsumers != nil {
		a.cancelConsumers()
	}
	return a.closer.Close(ctx)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := newApp(ctx)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	a.start()

	// run.Run blocks until SIGINT/SIGTERM, then calls stop to cancel goroutines
	// and close resources (closer is idempotent so the double call is safe).
	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.closer); err != nil {
		a.logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = a.stop(shutdownCtx) // cancel consumer goroutines; closer already ran above
	a.logger.Info("shutdown complete")
}
