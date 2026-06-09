// Package transport contains the Kafka consumer transport for the orders service.
package transport

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/pg"

	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// NewCommandHandler returns a kafka.HandlerFunc that decodes a
// CreateOrderCommand from the record, deduplicates via inbox.ProcessOnce, and
// dispatches to the decorated CQRS handler.
//
// Message-ID derivation: the real wire dedup key is the Kafka "message-id"
// header, which the outbox relay sets to the outbox Message.ID — a stable,
// unique UUID per event. The OrderId fallback (when the header is absent) is
// effectively dead in production but kept as a safety net for hand-crafted
// test messages that omit the header.
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
			return errors.New("orders consumer: cannot derive message id: no message-id header and order_id is empty")
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
