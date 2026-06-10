// Package transport contains the Kafka consumer transport for the payments
// service. Transport is DECODE + DISPATCH only: handlers here decode records
// (via consume.Typed) and dispatch to the app/domain layer; no business
// branching lives at this level — shared logic belongs to
// internal/domain/payment.Service ("cmd never calls cmd").
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
// OrderCreated records on the orders.events topic
// ("orders.OrderCreated.v1", derived from the proto message).
var OrderCreatedEventType = consume.EventTypeFor[*ordersv1.OrderCreated](1)

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
		consume.TypedFor(1, func(ctx context.Context, evt *ordersv1.OrderCreated) error {
			_, err := handler(ctx, app.ProcessPayment{
				OrderID:     evt.GetOrderId(),
				AmountCents: evt.GetAmountCents(),
				Currency:    evt.GetCurrency(),
			})
			return err
		}),
	)
}
