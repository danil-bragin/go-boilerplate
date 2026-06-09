// Package transport contains the Kafka consumer transport for the payments service.
package transport

import (
	"context"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// OrderCreatedEventType is the versioned event-type header value for
// OrderCreated records on the orders.events topic.
const OrderCreatedEventType = "orders.OrderCreated.v1"

// NewEventHandler returns a kafka.HandlerFunc that decodes an OrderCreated
// event from the record, deduplicates via the inbox, and dispatches to the
// decorated CQRS handler. All transport-level concerns (event-type dispatch,
// message-id policy, inbox dedup, principal headers) come from consume.Typed.
func NewEventHandler(
	pool *pg.Pool,
	handler func(context.Context, app.ProcessPayment) (app.ProcessPaymentResult, error),
	opts ...consume.Option,
) kafka.HandlerFunc {
	return consume.New(pool, "payments", opts...).Handler(
		consume.Typed(OrderCreatedEventType, func(ctx context.Context, evt *ordersv1.OrderCreated) error {
			_, err := handler(ctx, app.ProcessPayment{
				OrderID:     evt.GetOrderId(),
				AmountCents: evt.GetAmountCents(),
				Currency:    evt.GetCurrency(),
			})
			return err
		}),
	)
}
