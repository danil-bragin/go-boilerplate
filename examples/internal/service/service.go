// Package service provides a shared wiring harness for consumer services.
// It eliminates the per-service boilerplate of: logger, closer, pg pool,
// kafka client+producer, health checks, admin HTTP server (/livez /readyz /metrics),
// consumer wiring with poison-DLT, and outbox relay + cleanup.
//
// Cancel-before-close ordering:
//
//	Registration order in closer: log-sync, telemetry, pg, kafka-producer, admin-server, [consumers-cancel LAST]
//	Closer runs LIFO: consumers-cancel fires FIRST (stops goroutines), then admin-server shuts down,
//	then kafka-producer flushes+closes, then pg pool closes, then telemetry shuts down, then log syncs.
//	This ensures goroutines stop before the resources they use are torn down.
package service

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/platform/health"
	"go-boilerplate/platform/httpserver"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/outboxkafka"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"
)

// Config is the embeddable base config for all consumer services.
// Services embed this and add their own topic-name fields.
type Config struct {
	Log       log.Config
	Telemetry telemetry.Config
	PG        pg.Config
	Kafka     kafka.Config
	AdminAddr string `env:"ADMIN_HTTP_ADDR" envDefault:":9090"`
}

// goroutineFunc is a background goroutine registered via AddConsumer or AddOutboxRelay.
type goroutineFunc func(ctx context.Context)

// Service holds all shared wired components.
type Service struct {
	cfg         Config
	logger      *slog.Logger
	closer      *run.Closer
	pool        *pg.Pool
	kafkaClient *kgo.Client
	producer    *kafka.Producer
	h           *health.Health
	adminServer *httpserver.Server
	goroutines  []goroutineFunc
	runCtx      context.Context    //nolint:containedctx
	cancelRun   context.CancelFunc
}

// New wires all shared components: logger, telemetry, pg pool+migrations,
// kafka client+producer, health checks, and admin HTTP server with /livez, /readyz, /metrics.
//
// migrations and migrationsDir are passed to pg.Migrate (advisory-locked).
// Pass a nil fs.FS and empty string to skip migrations (useful in tests that
// handle migrations themselves).
func New(ctx context.Context, cfg Config, migrations fs.FS, migrationsDir string) (*Service, error) {
	// 1. Logger + log-sync (registered first → runs last in LIFO closer).
	logger, logSync := log.New(cfg.Log, os.Stdout)
	slog.SetDefault(logger)

	closer := run.NewCloser()
	closer.Add("log-sync", func(context.Context) error {
		_ = logSync()
		return nil
	})

	// 2. Telemetry + metrics handler.
	shutdownTel, metricsHandler, err := telemetry.SetupWithMetrics(ctx, cfg.Telemetry)
	if err != nil {
		return nil, err
	}
	closer.Add("telemetry", func(ctx context.Context) error {
		return shutdownTel(ctx)
	})

	// 3. Postgres pool.
	pool, err := pg.New(ctx, cfg.PG)
	if err != nil {
		return nil, err
	}
	closer.Add("pg", func(ctx context.Context) error {
		return pool.Close(ctx)
	})

	// 4. Migrations (advisory-locked; idempotent).
	if migrations != nil && migrationsDir != "" {
		if err := pg.Migrate(ctx, cfg.PG.DSN, migrations, migrationsDir); err != nil {
			return nil, err
		}
	}

	// 5. Kafka client (producer-only, no group) + producer.
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

	// 6. Health: pg + kafka readiness checks.
	h := health.New()
	h.AddReadiness("postgres", health.Check(func(ctx context.Context) error {
		return pool.HealthCheck(ctx)
	}))
	h.AddReadiness("kafka", health.Check(func(ctx context.Context) error {
		return producer.Ping(ctx)
	}))

	// 7. Admin HTTP server: /livez, /readyz, /metrics.
	adminAddr := cfg.AdminAddr
	if adminAddr == "" {
		adminAddr = ":9090"
	}
	adminServer := httpserver.New(httpserver.Config{Addr: adminAddr})
	health.Mount(adminServer.Mux(), h)
	if metricsHandler != nil {
		adminServer.Mux().Get("/metrics", metricsHandler.ServeHTTP)
	}
	closer.Add("admin-server", func(ctx context.Context) error {
		h.SetNotReady() // signal not-ready before draining
		return adminServer.Shutdown(ctx)
	})

	return &Service{
		cfg:         cfg,
		logger:      logger,
		closer:      closer,
		pool:        pool,
		kafkaClient: kafkaClient,
		producer:    producer,
		h:           h,
		adminServer: adminServer,
	}, nil
}

// Pool returns the Postgres pool.
func (s *Service) Pool() *pg.Pool { return s.pool }

// Producer returns the Kafka producer.
func (s *Service) Producer() *kafka.Producer { return s.producer }

// KafkaClient returns the underlying *kgo.Client (producer-only, no group).
func (s *Service) KafkaClient() *kgo.Client { return s.kafkaClient }

// Logger returns the service logger.
func (s *Service) Logger() *slog.Logger { return s.logger }

// Health returns the health aggregator.
func (s *Service) Health() *health.Health { return s.h }

// Closer returns the run.Closer for integration with run.Run.
func (s *Service) Closer() *run.Closer { return s.closer }

// Cfg returns the base Config.
func (s *Service) Cfg() Config { return s.cfg }

// EnsureTopics creates topics if they do not already exist (idempotent).
func (s *Service) EnsureTopics(ctx context.Context, partitions int32, rf int16, topics ...string) error {
	return kafka.EnsureTopics(ctx, s.kafkaClient, partitions, rf, topics...)
}

// AddConsumer wires a Kafka consumer: wraps handler with WithRetry (poison→DLT),
// creates a consumer for the group+topics, and registers its Run goroutine.
// DLT topics (topic+".DLT") are also created via EnsureTopics.
// Must be called before Start.
func (s *Service) AddConsumer(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc) error {
	// Ensure DLT topics exist alongside the source topics.
	allTopics := make([]string, 0, len(topics)*2)
	allTopics = append(allTopics, topics...)
	for _, t := range topics {
		allTopics = append(allTopics, t+".DLT")
	}
	if err := s.EnsureTopics(ctx, 1, 1, allTopics...); err != nil {
		return err
	}

	// Wrap with retry/DLT so poison messages never block the partition.
	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: 3,
		Producer:    s.producer,
		Backoff:     100 * time.Millisecond,
	})

	// Build consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics...)
	if err != nil {
		return err
	}
	// Register consumer Close in the closer (runs before consumers-cancel in LIFO,
	// but consumers-cancel is registered LAST so it runs FIRST — see ordering note at top).
	s.closer.Add("kafka-consumer-"+groupID, func(context.Context) error {
		consumer.Close()
		return nil
	})

	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := consumer.Run(ctx, wrapped); err != nil && ctx.Err() == nil {
			s.logger.Error("consumer stopped unexpectedly", "group", groupID, "error", err)
		}
	})
	return nil
}

// AddOutboxRelay wires an outbox relay + cleaner. Uses the passed publisher
// (typically outboxkafka.New(producer)). Must be called before Start.
func (s *Service) AddOutboxRelay(publisher outbox.Publisher, cfg outbox.RelayConfig) {
	relay := outbox.NewRelay(s.pool, publisher, cfg)
	relay.SetOnError(func(err error) {
		s.logger.Error("outbox relay error", "error", err)
	})

	cleaner := outbox.NewCleaner(s.pool)
	cleaner.SetOnError(func(err error) {
		s.logger.Error("outbox cleaner error", "error", err)
	})

	retention := cfg.RetentionAge
	if retention == 0 {
		retention = 24 * time.Hour
	}
	interval := cfg.CleanupInterval
	if interval == 0 {
		interval = time.Hour
	}

	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("relay stopped unexpectedly", "error", err)
		}
	})
	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := cleaner.RunCleanup(ctx, interval, retention); err != nil && ctx.Err() == nil {
			s.logger.Error("cleaner stopped unexpectedly", "error", err)
		}
	})
}

// DefaultOutboxPublisher builds the standard outboxkafka publisher backed by
// the service's producer. Convenience helper for services using the default
// topic-per-aggregate-type mapping.
func (s *Service) DefaultOutboxPublisher() outbox.Publisher {
	return outboxkafka.New(s.producer)
}

// Start starts the admin HTTP server and all registered consumer/relay/cleaner
// goroutines. Non-blocking.
//
// Admin server start failure is treated as a warning (logged, not returned) since
// the admin endpoint is observability-only and must not prevent service startup.
// This is important in tests where multiple services share the same default port.
//
// Ordering guarantee: a runCtx is created and cancelRun is registered as the
// LAST entry in the Closer (so it fires FIRST in LIFO teardown). This means
// goroutines receive context cancellation before the pg pool and kafka client
// are closed.
func (s *Service) Start() error {
	if err := s.adminServer.Start(); err != nil {
		// Non-fatal: log and continue. The admin endpoint is observability-only;
		// a port-bind failure (e.g. during tests with multiple services sharing
		// the default port) must not prevent the service from consuming messages.
		s.logger.Warn("admin server failed to start", "error", err, "addr", s.adminServer.Addr())
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	s.runCtx = runCtx
	s.cancelRun = cancelRun

	for _, g := range s.goroutines {
		fn := g // capture
		go fn(runCtx)
	}

	// Register cancelRun LAST → fires FIRST in LIFO closer, stopping goroutines
	// before the pg pool and kafka client are released.
	s.closer.Add("consumers-cancel", func(context.Context) error {
		cancelRun()
		return nil
	})

	s.logger.Info("service started", "admin_addr", s.adminServer.Addr())
	return nil
}

// Stop cancels consumer goroutines and closes all resources via the Closer.
func (s *Service) Stop(ctx context.Context) error {
	if s.cancelRun != nil {
		s.cancelRun()
	}
	return s.closer.Close(ctx)
}

// AdminAddr returns the actual bound admin server address (useful when :0 was used).
func (s *Service) AdminAddr() string { return s.adminServer.Addr() }
