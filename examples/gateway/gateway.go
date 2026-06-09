// Package gateway implements the HTTP edge service: accepts REST orders,
// publishes CreateOrderCommand to Kafka, and serves a read model built from
// event projections.
//
// # Harness
//
// Gateway is built on the shared service.Service harness (examples/internal/service)
// which handles the common wiring: logger, telemetry+metrics, pg pool+migrations,
// kafka client+producer, health checks, admin HTTP server (/livez /readyz /metrics).
// The gateway adds its own public REST server on cfg.HTTP.Addr.
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
//
// # Redis / cache
//
// When REDIS_ADDRS is set and reachable, gateway wires a two-tier Redis-backed
// cache (platform/cache) and applies the cqrs.CachingJSON behavior to the
// GetOrder query handler (30 s TTL). If cache.New fails (Redis unavailable),
// the gateway logs a warning and starts without caching — the GetOrder handler
// still works, just without a cache tier.
//
// # RBAC
//
// POST /orders requires the "user" or "admin" role when AuthDisabled=false.
// A principal without either role receives 403.
//
// # Resilience
//
// The Kafka command publish inside POST /orders is wrapped with a resilience
// policy (Retry×3 with 50 ms base back-off + 2 s timeout) so transient broker
// hiccups are retried automatically.
package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/open-feature/go-sdk/openfeature/memprovider"

	"go-boilerplate/examples/gateway/internal/api"
	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/examples/internal/service"
	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/blob"
	"go-boilerplate/platform/cache"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/health"
	"go-boilerplate/platform/httpserver"
	"go-boilerplate/platform/run"
)

// Config aggregates all configuration for the gateway service.
type Config struct {
	service.Config
	HTTP                httpserver.Config
	Cache               cache.Config
	S3                  blob.Config
	CommandsTopic       string `env:"GATEWAY_COMMANDS_TOPIC"        envDefault:"orders.commands"`
	OrdersEventsTopic   string `env:"GATEWAY_ORDERS_EVENTS_TOPIC"   envDefault:"orders.events"`
	PaymentsEventsTopic string `env:"GATEWAY_PAYMENTS_EVENTS_TOPIC" envDefault:"payments.events"`
	AuthDisabled        bool   `env:"GATEWAY_AUTH_DISABLED"         envDefault:"false"`
	JWKSUrl             string `env:"GATEWAY_JWKS_URL"              envDefault:""`
	JWKSIssuer          string `env:"GATEWAY_JWKS_ISSUER"           envDefault:""`
	JWKSAudience        string `env:"GATEWAY_JWKS_AUDIENCE"         envDefault:""`
	// CORSOrigins is the list of allowed CORS origins for the public HTTP server.
	// Use ["*"] for dev/demo. In production set explicit origins.
	// Default "*" allows any origin (demo-safe; auth should enforce identity).
	CORSOrigins []string `env:"GATEWAY_CORS_ORIGINS"          envSeparator:"," envDefault:"*"`
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
// NOTE: The harness always writes to os.Stdout; this option is kept for
// API compatibility with tests and e2e. Pass io.Discard to suppress logs.
func WithLogWriter(_ io.Writer) Option {
	return func(_ *App) {}
}

// App holds all wired components for the gateway service.
type App struct {
	svc      *service.Service
	server   *httpserver.Server
	verifier auth.Verifier
}

// NewApp wires all service components and returns a ready-to-start App.
// Call [App.Start] to begin serving and consuming, and [App.Stop] (or the
// run.Closer registered inside) to shut down gracefully.
func NewApp(ctx context.Context, opts ...Option) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	a := &App{}

	// Apply options before building components so overrides are in effect first.
	for _, o := range opts {
		o(a)
	}

	// Auth: resolve the verifier EARLY — before connecting to any external
	// services — so a misconfigured JWKS URL is caught immediately and the
	// service refuses to start (fail closed).
	if !cfg.AuthDisabled {
		if a.verifier == nil {
			v, err := auth.NewJWKSVerifier(ctx, cfg.JWKSUrl, cfg.JWKSIssuer, cfg.JWKSAudience)
			if err != nil {
				return nil, fmt.Errorf("gateway: building JWKS verifier (auth is enabled): %w", err)
			}
			a.verifier = v
		}
	}

	// Build the shared harness: logger, telemetry, pg+migrations, kafka, health, admin HTTP.
	svc, err := service.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}
	a.svc = svc

	if cfg.AuthDisabled {
		svc.Logger().Warn("gateway: auth is DISABLED — all endpoints are open; set GATEWAY_AUTH_DISABLED=false in production")
	}

	// Ensure Kafka topics exist.
	if err := svc.EnsureTopics(ctx, 1, 1,
		cfg.CommandsTopic,
		cfg.OrdersEventsTopic,
		cfg.PaymentsEventsTopic,
	); err != nil {
		return nil, err
	}

	// Cache (optional): try to build a Redis-backed two-tier cache.
	// If Redis is unreachable (empty addrs, connection refused, etc.) we log a
	// warning and continue without caching — the service degrades gracefully.
	var appCache cqrs.Cache // nil means caching disabled
	if len(cfg.Cache.RedisAddrs) > 0 && cfg.Cache.RedisAddrs[0] != "" {
		c, err := cache.New(cfg.Cache)
		if err != nil {
			svc.Logger().Warn("gateway: cache unavailable, starting without Redis caching",
				"error", err,
				"redis_addrs", cfg.Cache.RedisAddrs,
			)
		} else {
			appCache = c
			svc.Closer().Add("cache", func(ctx context.Context) error {
				return c.Close(ctx)
			})
			svc.Health().AddReadiness("cache", health.Check(func(ctx context.Context) error {
				return c.HealthCheck(ctx)
			}))
		}
	} else {
		svc.Logger().Info("gateway: REDIS_ADDRS not set, starting without cache")
	}

	// Blob / object-store (optional): try to build a MinIO-backed store for order
	// attachments. If the S3 endpoint is unreachable or the config is empty we log
	// a warning and continue without attachment support — graceful degradation.
	var objStore blob.ObjectStore
	if cfg.S3.Endpoint != "" {
		s, err := blob.New(ctx, cfg.S3)
		if err != nil {
			svc.Logger().Warn("gateway: blob/attachments disabled (S3 unavailable)",
				"error", err,
				"endpoint", cfg.S3.Endpoint,
			)
		} else {
			objStore = s
		}
	} else {
		svc.Logger().Info("gateway: S3_ENDPOINT not set, starting without blob/attachments")
	}

	// Feature flags (in-memory provider seeded with the order-attachments flag).
	// In production swap NewInMemory for a flagd / LaunchDarkly provider:
	//   _ = openfeature.SetNamedProviderAndWait("gateway", flagdProvider)
	//   flags := featureflags.New(openfeature.NewClient("gateway"))
	flags, err := featureflags.NewInMemory("gateway-"+cfg.HTTP.Addr, map[string]memprovider.InMemoryFlag{
		"order-attachments-enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants:       map[string]any{"on": true, "off": false},
		},
	})
	if err != nil {
		svc.Logger().Warn("gateway: feature flags unavailable, attachments will be disabled", "error", err)
	}

	// Build the CQRS GetOrder query handler (raw → decorated).
	rawGetOrder := gatewayapp.GetOrderHandler(svc.Pool())
	decoratedGetOrder := gatewayapp.DecorateGetOrderHandler(rawGetOrder, appCache)

	// Projection consumer subscribes to both events topics.
	projHandler := projection.NewHandler(svc.Pool(), svc.Logger())
	if err := svc.AddConsumer(ctx, "gateway-projection",
		[]string{cfg.OrdersEventsTopic, cfg.PaymentsEventsTopic},
		projHandler,
	); err != nil {
		return nil, err
	}

	// Public HTTP server (separate from the admin server on AdminAddr).
	httpSrv := httpserver.New(cfg.HTTP)
	svc.Closer().Add("http-server", func(ctx context.Context) error {
		return httpSrv.Shutdown(ctx)
	})
	a.server = httpSrv

	// Edge security: CORS (opt-in, configure origins via GATEWAY_CORS_ORIGINS)
	// and a global rate limiter (100 rps, burst 200).
	//
	// CORS is applied only when cfg.CORSOrigins is set. For demo/local the
	// default allows all origins. In production set GATEWAY_CORS_ORIGINS to
	// an explicit comma-separated list and remove the "*" wildcard.
	httpSrv.Mux().Use(httpserver.CORS(httpserver.CORSOptions{
		AllowedOrigins: cfg.CORSOrigins,
		AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization", "X-Request-Id"},
	}))
	// Global edge rate limiter: 100 rps sustained, burst 200.
	// NOTE: This is a process-local limiter. For multi-instance deployments
	// replace with a Redis-backed distributed rate limiter (GCRA).
	// For per-IP limiting, key the limiter by r.RemoteAddr (add an LRU map).
	httpSrv.Mux().Use(httpserver.RateLimit(100, 200))

	// Wire the API server (strict handler) with RBAC and resilience.
	apiServer := api.NewServer(
		svc.Pool(),
		svc.Producer(),
		cfg.CommandsTopic,
		svc.Logger(),
		decoratedGetOrder,
		cfg.AuthDisabled,
	)

	// Wrap handler errors: map authError → 403/401, others → 500.
	strictOpts := api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			if api.WriteAuthError(w, err) {
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
		},
	}
	strictHandler := api.NewStrictHandlerWithOptions(apiServer, nil, strictOpts)

	// Mount routes. When auth is enabled, apply auth middleware to all routes.
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

	// Attachment routes: mounted behind the same auth middleware so that
	// upload/download requires a valid token (same RBAC boundary as POST /orders).
	// When objStore is nil (S3 unavailable at startup), the routes are simply not
	// mounted — the feature is silently absent rather than returning 500.
	if objStore != nil && flags != nil {
		var attachRouter chi.Router
		if !cfg.AuthDisabled {
			attachMiddleware := auth.Middleware(a.verifier)
			attachRouter = httpSrv.Mux().With(func(next http.Handler) http.Handler {
				return attachMiddleware(next)
			})
		} else {
			attachRouter = httpSrv.Mux()
		}
		attachments.New(objStore, flags.Bool).Mount(attachRouter)
	}

	return a, nil
}

// Start launches background goroutines (projection consumer + public HTTP server).
// Non-blocking.
func (a *App) Start() {
	if err := a.svc.Start(); err != nil {
		a.svc.Logger().Error("failed to start service", "error", err)
	}

	if err := a.server.Start(); err != nil {
		a.svc.Logger().Error("http server failed to start", "error", err)
		return
	}

	a.svc.Logger().Info("gateway service started",
		"addr", a.server.Addr(),
		"admin_addr", a.svc.AdminAddr(),
	)
}

// Stop cancels consumer goroutines and closes all resources.
func (a *App) Stop(ctx context.Context) error {
	return a.svc.Stop(ctx)
}

// Closer returns the run.Closer for integration with run.Run.
func (a *App) Closer() *run.Closer {
	return a.svc.Closer()
}

// Addr returns the bound HTTP address (useful when :0 was used in tests).
func (a *App) Addr() string {
	return a.server.Addr()
}

// AdminAddr returns the bound admin HTTP server address (serves /livez, /readyz, /metrics).
func (a *App) AdminAddr() string {
	return a.svc.AdminAddr()
}

// KafkaClient returns the underlying Kafka client (used by tests that produce
// events directly into the broker).
func (a *App) KafkaClient() interface{} {
	return a.svc.KafkaClient()
}
