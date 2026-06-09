// Package transport contains the Kafka consumer transport for the payments service.
package transport

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/pg"

	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// NewEventHandler returns a kafka.HandlerFunc that decodes an OrderCreated event
// from the record, deduplicates via inbox.ProcessOnce, and dispatches to the
// decorated CQRS handler.
//
// Message-ID derivation: the real wire dedup key is the Kafka "message-id"
// header, which the orders service outbox relay sets to the outbox Message.ID —
// a stable, unique UUID per event. The OrderId fallback (when the header is
// absent) is effectively dead in production but kept as a safety net for
// hand-crafted test messages that omit the header.
func NewEventHandler(
	pool *pg.Pool,
	handler func(context.Context, app.ProcessPayment) (app.ProcessPaymentResult, error),
) kafka.HandlerFunc {
	return func(ctx context.Context, r kafka.Record) error {
		var evt ordersv1.OrderCreated
		if err := proto.Unmarshal(r.Value, &evt); err != nil {
			return fmt.Errorf("payments consumer: unmarshal event: %w", err)
		}

		// Derive the inbox message id.
		// Prefer the "message-id" header set by the outbox relay; fall back to
		// the order id so that duplicate events with the same order id are
		// deduplicated by the inbox pattern.
		msgID := r.Headers["message-id"]
		if msgID == "" {
			msgID = evt.GetOrderId()
		}
		if msgID == "" {
			return errors.New("payments consumer: cannot derive message id: no message-id header and order_id is empty")
		}

		_, err := inbox.ProcessOnce(ctx, pool, "payments", msgID, func(ctx context.Context) error {
			_, err := handler(ctx, app.ProcessPayment{
				OrderID:     evt.GetOrderId(),
				AmountCents: evt.GetAmountCents(),
				Currency:    evt.GetCurrency(),
			})
			return err
		})
		return err
	}
}
