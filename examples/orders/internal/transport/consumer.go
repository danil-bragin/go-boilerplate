// Package transport contains the Kafka consumer transport for the orders service.
package transport

import (
	"context"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// CommandEventType is the versioned event-type header value for
// CreateOrderCommand records on the orders.commands topic.
const CommandEventType = "orders.CreateOrderCommand.v1"

// NewCommandHandler returns a kafka.HandlerFunc that decodes a
// CreateOrderCommand from the record, deduplicates via the inbox, and
// dispatches to the decorated CQRS handler. All transport-level concerns
// (event-type dispatch, message-id policy, inbox dedup, principal headers)
// come from consume.Typed; see that package's documentation.
func NewCommandHandler(
	pool *pg.Pool,
	handler func(context.Context, app.CreateOrder) (app.CreateOrderResult, error),
	opts ...consume.Option,
) kafka.HandlerFunc {
	return consume.New(pool, "orders", opts...).Handler(
		consume.Typed(CommandEventType, func(ctx context.Context, cmd *ordersv1.CreateOrderCommand) error {
			_, err := handler(ctx, app.CreateOrder{
				OrderID:     cmd.GetOrderId(),
				CustomerID:  cmd.GetCustomerId(),
				AmountCents: cmd.GetAmountCents(),
				Currency:    cmd.GetCurrency(),
			})
			return err
		}),
	)
}
