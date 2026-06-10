// Package servicekit provides a shared wiring harness for consumer services.
// It eliminates the per-service boilerplate of: logger, closer, pg pool,
// kafka client+producer, health checks, admin HTTP server (/livez /readyz /metrics),
// consumer wiring with poison-DLT, and outbox relay + cleanup.
//
// Readiness-first teardown ordering:
//
//	Registration order in closer: admin-server FIRST, then log-sync, telemetry, pg,
//	kafka-producer, [service-specific closers], consumers-cancel, drain-gate LAST.
//	Closer runs LIFO, so teardown is:
//
//	  1. drain-gate      — /readyz flips to 503, then sleeps DRAIN_GRACE so load
//	                       balancers stop routing before anything shuts down.
//	  2. public HTTP servers (AddHTTPServer) — drain in-flight requests while
//	                       kafka/pg are still available to the handlers.
//	  3. consumers-cancel — stops goroutines and WAITS for them to exit.
//	  4. service closers  — caches, … (whatever the service registered after New).
//	  5. kafka-producer flushes+closes, pg pool closes, telemetry shuts down, log syncs.
//	  6. admin-server     — LAST, so /readyz keeps answering 503 for the entire drain.
package servicekit

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sync"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/serde"
	"go-boilerplate/platform/observability/health"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/observability/telemetry"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/web/httpserver"

	"github.com/grafana/pyroscope-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// goroutineFunc is a background goroutine registered via AddConsumer or AddOutboxRelay.
type goroutineFunc func(ctx context.Context)

// namedServer is a public HTTP server registered via AddHTTPServer.
type namedServer struct {
	name string
	srv  *httpserver.Server
}

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
	serde       *serde.Serde
	goroutines  []goroutineFunc
	httpServers []namedServer
	runCtx      context.Context //nolint:containedctx
	cancelRun   context.CancelFunc
	// fatal receives the first fatal serve error of a public HTTP server
	// (buffered, never closed). servicekit.Main watches it and tears the
	// process down with a non-zero exit — readiness alone only removes the
	// pod from endpoints, it never restarts it.
	fatal chan error
	// wg tracks every goroutine launched in Start. Teardown waits for them
	// to exit AFTER cancelling runCtx and BEFORE closing the kafka client /
	// pg pool, so in-flight final commits and cleanup writes are not cut off
	// by a closed client.
	wg sync.WaitGroup
}

// New wires all shared components: logger, telemetry, pg pool+migrations,
// kafka client+producer, health checks, and admin HTTP server with /livez, /readyz, /metrics.
//
// migrations and migrationsDir are passed to pg.Migrate (advisory-locked).
// Pass a nil fs.FS and empty string to skip migrations (useful in tests that
// handle migrations themselves).
//
// Options: WithoutKafka and WithoutPG switch off the respective subsystem
// entirely — no client construction, no dial, no readiness check; the
// dependent Add* methods return errors instead.
func New(ctx context.Context, cfg Config, migrations fs.FS, migrationsDir string, opts ...Option) (*Service, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	// 1. Telemetry FIRST — the logger needs the (opt-in, TELEMETRY_LOGS) OTel
	// log provider at construction time so records can fan out to the
	// collector in addition to stdout. Warnings telemetry emits during setup
	// go to the process-default logger; everything after this point uses the
	// service logger.
	tel, err := telemetry.SetupAll(ctx, cfg.Telemetry)
	if err != nil {
		return nil, err
	}
	metricsHandler := tel.MetricsHandler

	// 2. Logger + log-sync. WithOTelBridge ignores a nil provider, so this is
	// wiring-neutral when TELEMETRY_LOGS is off.
	logger, logSync := log.New(cfg.Log, os.Stdout, log.WithOTelBridge(tel.LoggerProvider))
	slog.SetDefault(logger)

	closer := run.NewCloser()

	// Admin-server teardown is registered FIRST so it runs LAST (LIFO): the
	// /readyz endpoint must keep serving 503 while everything else drains.
	// The server itself is constructed further down; the closure reads the
	// variable at teardown time, after New has assigned it.
	var adminServer *httpserver.Server
	closer.Add("admin-server", func(ctx context.Context) error {
		if adminServer == nil {
			return nil
		}
		return adminServer.Shutdown(ctx)
	})

	closer.Add("log-sync", func(context.Context) error {
		_ = logSync()
		return nil
	})

	// Telemetry closer registered AFTER log-sync: LIFO teardown shuts the
	// providers down (flushing the OTLP log batch) before stdout syncs.
	closer.Add("telemetry", func(ctx context.Context) error {
		return tel.Shutdown(ctx)
	})

	// Invariant check: the inbox dedup window must cover the broker's topic
	// retention. If a record can still be redelivered (retention not yet
	// expired) after its inbox row was cleaned up, the duplicate is no longer
	// detected. WARN loudly instead of failing — dev setups may not care.
	if cfg.TopicRetention > 0 && cfg.InboxRetention < cfg.TopicRetention {
		logger.Warn(
			"INBOX_RETENTION is shorter than TOPIC_RETENTION: "+
				"redelivered records older than the inbox window will NOT be deduplicated; "+
				"set INBOX_RETENTION >= TOPIC_RETENTION",
			"inbox_retention", cfg.InboxRetention,
			"topic_retention", cfg.TopicRetention,
		)
	}

	// 2b. Continuous profiling (opt-in via PYROSCOPE_ADDR). No-op when unset.
	if cfg.PyroscopeAddr != "" {
		appName := cfg.Telemetry.ServiceName
		if appName == "" {
			appName = "service" // mirror the OTEL_SERVICE_NAME default
		}
		profiler, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: appName,
			ServerAddress:   cfg.PyroscopeAddr,
			Logger:          nil, // background upload errors must not spam logs
		})
		if err != nil {
			return nil, fmt.Errorf("servicekit: starting pyroscope profiler: %w", err)
		}
		closer.Add("pyroscope", func(context.Context) error {
			return profiler.Stop() // flushes the final profile batch
		})
		logger.Info("pyroscope continuous profiling enabled", "addr", cfg.PyroscopeAddr)
	}

	// 3. Postgres pool (skipped entirely with WithoutPG — no dial).
	var pool *pg.Pool
	if !o.withoutPG {
		pool, err = pg.New(ctx, cfg.PG)
		if err != nil {
			return nil, err
		}
		pgPool := pool
		closer.Add("pg", func(ctx context.Context) error {
			return pgPool.Close(ctx)
		})

		// 4. Migrations (advisory-locked; idempotent). MIGRATE_ON_START=false
		// skips them (production: a dedicated migrate job owns schema changes).
		// MigrateDSN honors PG_MIGRATE_URL — required behind PgBouncer.
		if cfg.MigrateOnStart && migrations != nil && migrationsDir != "" {
			if err := pg.Migrate(ctx, cfg.PG.MigrateDSN().Reveal(), migrations, migrationsDir); err != nil {
				return nil, err
			}
		}
	}

	// 5. Kafka client (producer-only, no group) + producer (skipped entirely
	// with WithoutKafka — no client construction).
	var (
		kafkaClient *kgo.Client
		producer    *kafka.Producer
	)
	if !o.withoutKafka {
		producerCfg := cfg.Kafka
		producerCfg.GroupID = ""
		kafkaClient, err = kafka.NewClient(producerCfg)
		if err != nil {
			return nil, err
		}
		producer = kafka.NewProducer(kafkaClient)
		kProducer := producer
		closer.Add("kafka-producer", func(ctx context.Context) error {
			return kProducer.Close(ctx)
		})
	}

	// 6. Health: pg + kafka readiness checks (only for the wired subsystems).
	h := health.New()
	if pool != nil {
		h.AddReadiness("postgres", health.Check(func(ctx context.Context) error {
			return pool.HealthCheck(ctx)
		}))
	}
	if producer != nil {
		h.AddReadiness("kafka", health.Check(func(ctx context.Context) error {
			return producer.Ping(ctx)
		}))
	}

	// 7. Admin HTTP server: /livez, /readyz, /metrics.
	adminAddr := cfg.AdminAddr
	if adminAddr == "" {
		adminAddr = ":9090"
	}
	adminServer = httpserver.New(httpserver.Config{Addr: adminAddr})
	health.Mount(adminServer.Mux(), h)
	if metricsHandler != nil {
		adminServer.Mux().Get("/metrics", metricsHandler.ServeHTTP)
	}
	// NOTE: the admin-server closer was registered FIRST (top of New) so it
	// shuts down last; SetNotReady happens in the drain-gate closer (Start).

	// 8. Optional Schema Registry serde (SERDE_SR_URL). Construction is
	// offline; schema registration happens via RegisterSchema (fail-fast).
	var srSerde *serde.Serde
	if cfg.SerdeSRURL != "" {
		srSerde, err = serde.New(cfg.SerdeSRURL)
		if err != nil {
			return nil, err
		}
	}

	return &Service{
		cfg:         cfg,
		logger:      logger,
		closer:      closer,
		pool:        pool,
		kafkaClient: kafkaClient,
		producer:    producer,
		h:           h,
		adminServer: adminServer,
		serde:       srSerde,
		fatal:       make(chan error, 1),
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

// Fatal returns the channel that receives the first fatal serve error of a
// public HTTP server (the listener dying after a successful Start). It
// satisfies the FatalNotifier contract servicekit.Main watches: on a fatal
// error Main tears the app down via the Closer and exits non-zero, so the
// orchestrator restarts the pod instead of leaving a NotReady zombie (the
// liveness probe stays green — readiness alone never restarts anything).
//
// App wrappers around *Service should expose this method (delegate to the
// embedded Service) so Main can see fatal errors through them.
func (s *Service) Fatal() <-chan error { return s.fatal }

// Cfg returns the base Config.
func (s *Service) Cfg() Config { return s.cfg }

// AdminAddr returns the actual bound admin server address (useful when :0 was used).
func (s *Service) AdminAddr() string { return s.adminServer.Addr() }

// AddWorker registers a named background goroutine that Start launches and
// teardown cancels/waits like any consumer goroutine. fn must return promptly
// when ctx is cancelled. Must be called before Start.
func (s *Service) AddWorker(name string, fn func(context.Context)) {
	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		s.logger.Debug("worker started", "worker", name)
		fn(ctx)
	})
}

// AddHTTPServer registers a public HTTP server under the service lifecycle:
// Start binds and serves it (bind failure is fatal, same contract as the
// admin server), and teardown shuts it down gracefully in the slot between
// the drain-gate and consumers-cancel:
//
//	drain-gate (readyz→503 + DRAIN_GRACE) → public HTTP shutdown (drains
//	in-flight requests) → consumers-cancel → kafka/pg close → admin last.
//
// In-flight request handlers may therefore still use the kafka producer and
// pg pool while the server drains. A fatal serve error after startup (e.g.
// the listener dying) flips readiness to 503 AND is forwarded to Fatal();
// under servicekit.Main that triggers full teardown and a non-zero exit so
// the orchestrator restarts the process (readiness alone never would).
// Must be called before Start.
func (s *Service) AddHTTPServer(name string, srv *httpserver.Server) {
	s.httpServers = append(s.httpServers, namedServer{name: name, srv: srv})
}
