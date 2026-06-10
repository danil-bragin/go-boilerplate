// Package gateway implements the HTTP edge service: accepts REST orders,
// publishes CreateOrderCommand to Kafka, and serves a read model built from
// event projections.
//
// # Harness
//
// Gateway is built on the shared servicekit.Service harness (platform/servicekit)
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
// POST /v1/orders requires the "user" or "admin" role when AuthDisabled=false.
// A principal without either role receives 403.
//
// # Resilience
//
// The Kafka command publish inside POST /v1/orders is wrapped with a resilience
// policy (Retry×3 with 50 ms base back-off + 2 s timeout) so transient broker
// hiccups are retried automatically.
package gateway

import (
	"context"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/web/httpserver"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Option is a functional option for [NewApp].
type Option func(*App)

// WithVerifier injects a custom auth.Verifier (useful for tests or when auth
// is managed externally). When set, this overrides the JWKS-backed verifier.
func WithVerifier(v auth.Verifier) Option {
	return func(a *App) {
		a.verifier = v
	}
}

// App holds all wired components for the gateway service.
type App struct {
	svc      *servicekit.Service
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
	a.verifier, err = buildVerifier(ctx, cfg, a.verifier)
	if err != nil {
		return nil, err
	}

	// Build the shared harness: logger, telemetry, pg+migrations, kafka, health, admin HTTP.
	svc, err := servicekit.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}
	a.svc = svc

	if cfg.AuthDisabled {
		svc.Logger().Warn("gateway: auth is DISABLED — all endpoints are open; set GATEWAY_AUTH_DISABLED=false in production")
	}

	// Ensure Kafka topics exist.
	if err := svc.EnsureTopics(
		ctx,
		cfg.CommandsTopic,
		cfg.OrdersEventsTopic,
		cfg.PaymentsEventsTopic,
	); err != nil {
		return nil, err
	}

	// Schema Registry (no-op when SERDE_SR_URL is unset): commands the gateway
	// produces and events the projection consumes.
	if err := svc.RegisterSchema(ctx, cfg.CommandsTopic, "orders.CreateOrderCommand.v1", &ordersv1.CreateOrderCommand{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.OrdersEventsTopic, projection.OrderCreatedEventType, &ordersv1.OrderCreated{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.OrdersEventsTopic, projection.OrderPaymentTimedOutEventType, &ordersv1.OrderPaymentTimedOut{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.PaymentsEventsTopic, projection.PaymentProcessedEventType, &ordersv1.PaymentProcessed{}); err != nil {
		return nil, err
	}
	if err := svc.RegisterSchema(ctx, cfg.PaymentsEventsTopic, projection.PaymentFailedEventType, &ordersv1.PaymentFailed{}); err != nil {
		return nil, err
	}

	// Parse trusted-proxy CIDRs (fail fast on invalid CIDR).
	trustedPrefixes, err := ParseTrustedProxies(cfg.TrustedProxies)
	if err != nil {
		return nil, err
	}

	// Optional dependencies (all degrade gracefully on failure).
	appCache := buildCache(cfg, svc)
	objStore := buildBlob(ctx, cfg, svc)
	flags := buildFeatureFlags(cfg, svc)

	// Build the per-IP rate limiter (memory or Redis).
	limiter := buildLimiter(cfg, svc)

	// Build the CQRS query handlers (raw → decorated).
	rawGetOrder := gatewayapp.GetOrderHandler(svc.Pool())
	decoratedGetOrder := gatewayapp.DecorateGetOrderHandler(rawGetOrder, appCache)
	decoratedListOrders := gatewayapp.DecorateListOrdersHandler(gatewayapp.ListOrdersHandler(svc.Pool()))

	// Projection consumer subscribes to both events topics. With
	// GATEWAY_EMBEDDED_PROJECTION=false the consumer is NOT registered here —
	// the standalone projection binary (cmd/projection) owns the group
	// instead, and this process is a pure HTTP edge.
	if cfg.EmbeddedProjection {
		var consumeOpts []consume.Option
		if sd := svc.Serde(); sd != nil {
			consumeOpts = append(consumeOpts, consume.WithSerde(sd))
		}
		projHandler := projection.NewHandler(svc.Pool(), svc.Logger(), appCache, consumeOpts...)
		if err := svc.AddConsumer(
			ctx, "gateway-projection",
			[]string{cfg.OrdersEventsTopic, cfg.PaymentsEventsTopic},
			projHandler,
		); err != nil {
			return nil, err
		}
	} else {
		svc.Logger().Info("gateway: embedded projection disabled — expecting a standalone projection deployment (cmd/projection)")
	}

	// Public HTTP server (separate from the admin server on AdminAddr).
	// AddHTTPServer owns its lifecycle: Start binds it (bind failure fatal,
	// like the admin server) and teardown shuts it down right after the
	// drain-gate — before consumers are cancelled and kafka/pg close.
	httpSrv := httpserver.New(cfg.HTTP)
	svc.AddHTTPServer("public", httpSrv)
	a.server = httpSrv

	// Wire the API server (strict handler) with RBAC and resilience.
	apiServer := api.NewServer(
		svc.Pool(),
		svc.Producer(),
		cfg.CommandsTopic,
		svc.Logger(),
		decoratedGetOrder,
		decoratedListOrders,
		cfg.AuthDisabled,
	)
	if sd := svc.Serde(); sd != nil {
		apiServer.SetEncoder(sd)
	}

	// Apply edge security and mount all routes.
	applyEdgeSecurity(cfg, httpSrv.Mux(), limiter, trustedPrefixes)
	mountAPIRoutes(cfg, httpSrv.Mux(), apiServer, a.verifier)
	mountAttachmentRoutes(cfg, httpSrv, a.verifier, objStore, flags, svc.Pool())

	return a, nil
}

// Start launches background goroutines (projection consumer + public HTTP
// server, both managed by the servicekit harness). Non-blocking.
func (a *App) Start() error {
	if err := a.svc.Start(); err != nil {
		return err
	}

	a.svc.Logger().Info(
		"gateway service started",
		"addr", a.server.Addr(),
		"admin_addr", a.svc.AdminAddr(),
	)
	return nil
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
