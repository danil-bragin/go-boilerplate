package gateway

import (
	"context"
	"log/slog"

	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/servicekit"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// ProjectionApp is the standalone read-model projection service: the gateway
// projection consumer extracted into its own deployable binary
// (examples/gateway/cmd/projection). It runs NO public HTTP server — only the
// servicekit admin server (/livez /readyz /metrics) plus the projection
// consumer over orders.events + payments.events.
//
// It shares the gateway's database (orders_read + inbox migrations), consumer
// group ("gateway-projection"), and cache-busting behavior, so the gateway
// can flip GATEWAY_EMBEDDED_PROJECTION=false and hand the group over without
// reprocessing or duplicate side effects (inbox dedup spans both modes).
type ProjectionApp struct {
	svc *servicekit.Service
}

// NewProjectionApp wires the consumer-only projection service. It reads the
// same environment variables as the gateway (Config), ignoring the HTTP/auth
// fields that only matter to the edge.
func NewProjectionApp(ctx context.Context) (*ProjectionApp, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	svc, err := servicekit.New(ctx, cfg.Config, migrations.FS, "sql")
	if err != nil {
		return nil, err
	}

	if err := svc.EnsureTopics(ctx, cfg.OrdersEventsTopic, cfg.PaymentsEventsTopic); err != nil {
		return nil, err
	}

	// Schema Registry (no-op when SERDE_SR_URL is unset).
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

	// Cache busting works the same as in embedded mode: when Redis is
	// configured the projection deletes stale order-view entries after each
	// committed write (nil cache = no-op, graceful degradation).
	appCache := buildCache(cfg, svc)

	// SSE status notifications also work the same as in embedded mode: the
	// standalone projection PUBLISHes status changes to Redis; the gateway
	// edge instances hold the SSE subscriptions. With no Redis configured
	// Notify is a no-op and edges fall back to store polling.
	streamer := buildSSE(cfg, svc)

	var consumeOpts []consume.Option
	if sd := svc.Serde(); sd != nil {
		consumeOpts = append(consumeOpts, consume.WithSerde(sd))
	}
	if cfg.ProjectionBatch {
		// Batch-apply mode (default, GATEWAY_PROJECTION_BATCH=true): one tx per
		// partition-batch-per-poll. perRecord is the per-event fallback;
		// AddBatchConsumer wraps it in WithRetry/DLT, so the failure contract
		// (poison → <topic>.DLT) matches per-event mode.
		perRecord := projection.NewHandler(svc.Shards(), svc.Logger(), appCache, streamer.Notify, consumeOpts...)
		build := func(fb kafka.HandlerFunc) kafka.BatchHandlerFunc {
			return projection.NewBatchHandler(svc.Shards(), svc.Logger(), appCache, streamer.Notify, fb, consumeOpts...)
		}
		if err := svc.AddBatchConsumer(
			ctx, "gateway-projection",
			[]string{cfg.OrdersEventsTopic, cfg.PaymentsEventsTopic},
			perRecord, build,
		); err != nil {
			return nil, err
		}
	} else {
		projHandler := projection.NewHandler(svc.Shards(), svc.Logger(), appCache, streamer.Notify, consumeOpts...)
		if err := svc.AddConsumer(
			ctx, "gateway-projection",
			[]string{cfg.OrdersEventsTopic, cfg.PaymentsEventsTopic},
			projHandler,
		); err != nil {
			return nil, err
		}
	}

	return &ProjectionApp{svc: svc}, nil
}

// Start launches the projection consumer and admin server. Non-blocking.
func (a *ProjectionApp) Start() error {
	if err := a.svc.Start(); err != nil {
		return err
	}
	a.svc.Logger().Info("projection service started", "admin_addr", a.svc.AdminAddr())
	return nil
}

// Stop cancels the consumer and closes all resources.
func (a *ProjectionApp) Stop(ctx context.Context) error {
	return a.svc.Stop(ctx)
}

// Closer returns the run.Closer for integration with run.Run.
func (a *ProjectionApp) Closer() *run.Closer { return a.svc.Closer() }

// Logger returns the service logger.
func (a *ProjectionApp) Logger() *slog.Logger { return a.svc.Logger() }

// AdminAddr returns the bound admin HTTP address (/livez /readyz /metrics).
func (a *ProjectionApp) AdminAddr() string { return a.svc.AdminAddr() }
