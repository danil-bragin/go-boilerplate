package transport

import (
	"context"
	"log/slog"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event types consumed from payments.events by the orders service
// (derived from the proto messages via consume.EventTypeFor).
var (
	PaymentProcessedEventType = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	PaymentFailedEventType    = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
)

// PaymentOutcomeApplier is the slice of order.Service this consumer
// dispatches to (consumer-side interface; tests inject a capturing fake).
type PaymentOutcomeApplier interface {
	ApplyPaymentOutcome(ctx context.Context, orderID string, outcome order.Status) error
}

// NewPaymentsEventHandler returns a kafka.HandlerFunc that maps terminal
// payment events to order statuses: PaymentProcessed → StatusPaid,
// PaymentFailed → StatusPaymentFailed. It is decode+dispatch ONLY — the
// business rules (state machine, first outcome wins, the compensation warn)
// live in order.Service.ApplyPaymentOutcome; transport-level concerns
// (event-type dispatch, message-id policy, inbox dedup, principal headers)
// come from consume.Typed.
//
// This consumer exists so the unpaid-order watcher (app.UnpaidWatcher) can be
// a pure local query over the orders table — no cross-service/database lookup
// to decide whether an order was paid.
func NewPaymentsEventHandler(pool *pg.Pool, svc PaymentOutcomeApplier, logger *slog.Logger, opts ...consume.Option) kafka.HandlerFunc {
	opts = append([]consume.Option{consume.WithLogger(logger)}, opts...)
	return consume.New(pg.WrapPool(pool), "orders-payments", opts...).Handler(
		consume.TypedFor(1, func(ctx context.Context, evt *ordersv1.PaymentProcessed) error {
			return svc.ApplyPaymentOutcome(ctx, evt.GetOrderId(), order.StatusPaid)
		}),
		consume.TypedFor(1, func(ctx context.Context, evt *ordersv1.PaymentFailed) error {
			return svc.ApplyPaymentOutcome(ctx, evt.GetOrderId(), order.StatusPaymentFailed)
		}),
	)
}
