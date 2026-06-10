package transport

import (
	"context"
	"fmt"
	"log/slog"

	"go-boilerplate/examples/orders/internal/store/gen"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event types consumed from payments.events by the orders service
// (derived from the proto messages via consume.EventTypeFor).
var (
	PaymentProcessedEventType = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	PaymentFailedEventType    = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
)

// NewPaymentsEventHandler returns a kafka.HandlerFunc that records terminal
// payment outcomes on the orders service's OWN order rows: PaymentProcessed →
// status 'paid', PaymentFailed → status 'payment_failed'. Transitions apply
// only from 'created' (first outcome wins) and are inbox-deduped via
// consume.Typed.
//
// This consumer exists so the unpaid-order watcher (app.UnpaidWatcher) can be
// a pure local query over the orders table — no cross-service/database lookup
// to decide whether an order was paid.
func NewPaymentsEventHandler(pool *pg.Pool, logger *slog.Logger, opts ...consume.Option) kafka.HandlerFunc {
	apply := func(ctx context.Context, rawOrderID, status string) error {
		orderID, err := uuid.Parse(rawOrderID)
		if err != nil {
			return fmt.Errorf("orders: parse order_id %q: %w", rawOrderID, err)
		}
		rows, err := gen.New(pg.FromContext(ctx, pool)).MarkOrderPaymentOutcome(ctx, gen.MarkOrderPaymentOutcomeParams{
			ID:     orderID,
			Status: status,
		})
		if err != nil {
			return fmt.Errorf("orders: mark payment outcome %s=%s: %w", orderID, status, err)
		}
		if rows == 0 {
			// Row missing (event for an unknown order) or already in a
			// terminal status — first outcome wins; log and move on.
			//
			// Sharpest variant: status='paid' arriving for an order already
			// in 'payment_timeout' means the customer WAS charged but the
			// order timed out. The order stays timed out in both stores by
			// design; the charge needs compensation (refund / manual review,
			// ADR-0007) — this warn line is the operational signal.
			logger.Warn("orders: payment outcome ignored (order not in 'created'; a 'paid' outcome on a timed-out order means the charge needs compensation)",
				"order_id", orderID, "status", status)
		}
		return nil
	}

	opts = append([]consume.Option{consume.WithLogger(logger)}, opts...)
	return consume.New(pool, "orders-payments", opts...).Handler(
		consume.TypedFor(1, func(ctx context.Context, evt *ordersv1.PaymentProcessed) error {
			return apply(ctx, evt.GetOrderId(), "paid")
		}),
		consume.TypedFor(1, func(ctx context.Context, evt *ordersv1.PaymentFailed) error {
			return apply(ctx, evt.GetOrderId(), "payment_failed")
		}),
	)
}
