// Package gateway implements the HTTP edge service: accepts REST orders,
// publishes CreateOrderCommand to Kafka, and serves a read model built from
// event projections.
//
// # Testability
//
// The App struct accepts functional options. Use [WithVerifier] to inject a
// custom auth.Verifier in tests instead of the default JWKS-backed verifier:
//
//	app, err := gateway.NewApp(ctx,
//	    gateway.WithVerifier(myStubVerifier),
//	)
//
// # Auth behaviour
//
// When AuthDisabled=false (the default), [NewApp] MUST have a verifier.
// If [WithVerifier] was not called, NewApp builds a [auth.JWKSVerifier] from
// the JWKSUrl/JWKSIssuer/JWKSAudience config fields. If that construction
// fails (empty URL, unreachable endpoint, etc.) NewApp returns an error and the
// service does NOT start — fail-closed, never open.
// Set GATEWAY_AUTH_DISABLED=true only for dev/demo; a WARN is logged.
package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/httpserver"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"
)

// Config aggregates all configuration for the gateway service.
type Config struct {
	Log                 log.Config
	Telemetry           telemetry.Config
	PG                  pg.Config
	Kafka               kafka.Config
	HTTP                httpserver.Config
	CommandsTopic       string `env:"GATEWAY_COMMANDS_TOPIC"        envDefault:"orders.commands"`
	OrdersEventsTopic   string `env:"GATEWAY_ORDERS_EVENTS_TOPIC"   envDefault:"orders.events"`
	PaymentsEventsTopic string `env:"GATEWAY_PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
	AuthDisabled        bool   `env:"GATEWAY_AUTH_DISABLED"         envDefault:"false"`
	JWKSUrl             string `env:"GATEWAY_JWKS_URL"              envDefault:""`
	JWKSIssuer          string `env:"GATEWAY_JWKS_ISSUER"           envDefault:""`
	JWKSAudience        string `env:"GATEWAY_JWKS_AUDIENCE"         envDefault:""`
}

// Option is a functional option for [NewApp].
type Option func(*App)

// WithVerifier injects a custom auth.Verifier (useful for tests or when auth
// is managed externally). When set, this overrides the JWKS-backed verifier.
func WithVerifier(v auth.Verifier) Option {
	return func(a *App) {
		a.verifier = v
	}
}

// WithLogWriter overrides the log output writer (default: os.Stdout).
func WithLogWriter(w io.Writer) Option {
	return func(a *App) {
		a.logWriter = w
	}
}

// App holds all wired components for the gateway service.
type App struct {
	cfg             Config
	logger          *slog.Logger
	closer          *run.Closer
	server          *httpserver.Server
	projConsumer    *kafka.Consumer
	projHandler     kafka.HandlerFunc
	cancelConsumers context.CancelFunc
	verifier        auth.Verifier
	logWriter       io.Writer
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin serving and consuming, and [App.Stop] (or the
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

	// Apply options before building components so overrides are in effect first.
	for _, o := range opts {
		o(a)
	}

	// Auth: resolve the verifier EARLY — before connecting to any external
	// services — so a misconfigured JWKS URL is caught immediately and the
	// service refuses to start (fail closed). This also means the fail-closed
	// unit test does not require a running postgres/kafka instance.
	if !cfg.AuthDisabled {
		if a.verifier == nil {
			// Build a JWKS-backed verifier from config. Any error here (empty
			// URL, unreachable endpoint, …) is fatal: we refuse to serve open.
			v, err := auth.NewJWKSVerifier(ctx, cfg.JWKSUrl, cfg.JWKSIssuer, cfg.JWKSAudience)
			if err != nil {
				return nil, fmt.Errorf("gateway: building JWKS verifier (auth is enabled): %w", err)
			}
			a.verifier = v
		}
	}

	logger, logSync := log.New(cfg.Log, a.logWriter)
	a.logger = logger
	slog.SetDefault(logger)

	if cfg.AuthDisabled {
		logger.Warn("gateway: auth is DISABLED — all endpoints are open; set GATEWAY_AUTH_DISABLED=false in production")
	}

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

	// Kafka client for producer (no consumer group).
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
		cfg.CommandsTopic,
		cfg.OrdersEventsTopic,
		cfg.PaymentsEventsTopic,
	); err != nil {
		return nil, err
	}

	// Projection consumer subscribes to both events topics.
	consumerCfg := cfg.Kafka
	consumerCfg.GroupID = "gateway-projection"
	projConsumer, err := kafka.NewConsumer(consumerCfg,
		cfg.OrdersEventsTopic,
		cfg.PaymentsEventsTopic,
	)
	if err != nil {
		return nil, err
	}
	closer.Add("kafka-projection-consumer", func(context.Context) error {
		projConsumer.Close()
		return nil
	})
	a.projConsumer = projConsumer
	a.projHandler = projection.NewHandler(pool, logger)

	// HTTP server.
	httpSrv := httpserver.New(cfg.HTTP)
	closer.Add("http-server", func(ctx context.Context) error {
		return httpSrv.Shutdown(ctx)
	})
	a.server = httpSrv

	// Wire the API server (strict handler).
	apiServer := api.NewServer(pool, producer, cfg.CommandsTopic, logger)
	strictHandler := api.NewStrictHandler(apiServer, nil)

	// Mount routes. When auth is enabled, a.verifier is guaranteed non-nil
	// (fail-closed above ensures this). Apply auth middleware to all routes.
	chiOpts := api.ChiServerOptions{
		BaseRouter: httpSrv.Mux(),
	}
	if !cfg.AuthDisabled {
		authMiddleware := auth.Middleware(a.verifier)
		chiOpts.Middlewares = []api.MiddlewareFunc{
			func(next http.Handler) http.Handler {
				return authMiddleware(next)
			},
		}
	}
	api.HandlerWithOptions(strictHandler, chiOpts)

	return a, nil
}

// Start launches background goroutines (projection consumer + HTTP server).
// Non-blocking.
func (a *App) Start() {
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancelConsumers = cancel

	// Register the cancel as the LAST entry in the Closer so it runs FIRST
	// during reverse-order teardown — goroutines stop before their resources
	// (pg pool, kafka client) are closed.
	a.closer.Add("consumers-cancel", func(context.Context) error {
		cancel()
		return nil
	})

	// Start projection consumer.
	go func() {
		if err := a.projConsumer.Run(runCtx, a.projHandler); err != nil && runCtx.Err() == nil {
			a.logger.Error("projection consumer stopped unexpectedly", "error", err)
		}
	}()

	// Start HTTP server.
	if err := a.server.Start(); err != nil {
		a.logger.Error("http server failed to start", "error", err)
		return
	}

	a.logger.Info("gateway service started", "addr", a.server.Addr())
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

// Addr returns the bound HTTP address (useful when Addr used :0 in tests).
func (a *App) Addr() string {
	return a.server.Addr()
}
