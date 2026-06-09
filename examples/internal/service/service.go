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

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/observability/health"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/observability/telemetry"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/web/httpserver"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
	runCtx      context.Context //nolint:containedctx
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

// AdminAddr returns the actual bound admin server address (useful when :0 was used).
func (s *Service) AdminAddr() string { return s.adminServer.Addr() }
