// Package transport contains the Kafka consumer transport for the orders service.
package transport

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"go-boilerplate/examples/orders/internal/app"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
	"go-boilerplate/platform/inbox"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/pg"
)

// NewCommandHandler returns a kafka.HandlerFunc that decodes a
// CreateOrderCommand from the record, deduplicates via inbox.ProcessOnce, and
// dispatches to the decorated CQRS handler.
//
// Message-ID derivation: the Kafka header "message-id" is used when present
// (set by upstream producers). Otherwise the command's OrderId is used as the
// idempotency key. Using the OrderId means that reprocessing the same order
// command (same OrderId) is deduplicated by the inbox table — producing
// exactly-once order creation even under at-least-once Kafka delivery.
func NewCommandHandler(
	pool *pg.Pool,
	handler func(context.Context, app.CreateOrder) (app.CreateOrderResult, error),
) kafka.HandlerFunc {
	return func(ctx context.Context, r kafka.Record) error {
		var cmd ordersv1.CreateOrderCommand
		if err := proto.Unmarshal(r.Value, &cmd); err != nil {
			return fmt.Errorf("orders consumer: unmarshal command: %w", err)
		}

		// Derive the inbox message id.
		// Prefer the "message-id" header set by the producer; fall back to
		// the order id so that duplicate commands with the same order id are
		// deduplicated by the inbox pattern.
		msgID := r.Headers["message-id"]
		if msgID == "" {
			msgID = cmd.GetOrderId()
		}
		if msgID == "" {
			return fmt.Errorf("orders consumer: cannot derive message id: no message-id header and order_id is empty")
		}

		_, err := inbox.ProcessOnce(ctx, pool, "orders", msgID, func(ctx context.Context) error {
			_, err := handler(ctx, app.CreateOrder{
				OrderID:     cmd.GetOrderId(),
				CustomerID:  cmd.GetCustomerId(),
				AmountCents: cmd.GetAmountCents(),
				Currency:    cmd.GetCurrency(),
			})
			return err
		})
		return err
	}
}
