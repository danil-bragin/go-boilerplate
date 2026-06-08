// Package projection implements the gateway read-model projection consumer.
// It consumes from "orders.events" and "payments.events", deduplicating via inbox,
// and upserts/updates the orders_read table.
package projection

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
	"go-boilerplate/platform/inbox"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/pg"
)

const consumerGroup = "gateway-projection"

// NewHandler returns a kafka.HandlerFunc that handles both "orders.events" and
// "payments.events" records, routing on the "event-type" header.
func NewHandler(pool *pg.Pool, logger *slog.Logger) kafka.HandlerFunc {
	return func(ctx context.Context, r kafka.Record) error {
		eventType := r.Headers["event-type"]

		// Derive the inbox message id from the "message-id" header.
		msgID := r.Headers["message-id"]
		if msgID == "" {
			// Fallback: use topic + key as a composite id.
			msgID = r.Topic + "-" + string(r.Key)
		}
		if msgID == "" {
			return fmt.Errorf("projection: cannot derive message id for event-type=%s", eventType)
		}

		switch eventType {
		case "OrderCreated":
			return handleOrderCreated(ctx, pool, logger, msgID, r.Value)
		case "PaymentProcessed":
			return handlePaymentProcessed(ctx, pool, logger, msgID, r.Value)
		default:
			// Unknown event type — skip silently (forward-compatible).
			logger.Debug("projection: skipping unknown event type", "event_type", eventType, "topic", r.Topic)
			return nil
		}
	}
}

func handleOrderCreated(ctx context.Context, pool *pg.Pool, logger *slog.Logger, msgID string, value []byte) error {
	var evt ordersv1.OrderCreated
	if err := proto.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("projection: unmarshal OrderCreated: %w", err)
	}

	orderID, err := uuid.Parse(evt.GetOrderId())
	if err != nil {
		return fmt.Errorf("projection: parse order_id %q: %w", evt.GetOrderId(), err)
	}

	_, err = inbox.ProcessOnce(ctx, pool, consumerGroup, msgID, func(ctx context.Context) error {
		q := storegen.New(pg.FromContext(ctx, pool))
		return q.UpsertOrderCreated(ctx, storegen.UpsertOrderCreatedParams{
			OrderID:     orderID,
			CustomerID:  evt.GetCustomerId(),
			AmountCents: evt.GetAmountCents(),
			Currency:    evt.GetCurrency(),
		})
	})
	if err != nil {
		return fmt.Errorf("projection: process OrderCreated: %w", err)
	}
	logger.Debug("projection: upserted OrderCreated", "order_id", orderID)
	return nil
}

func handlePaymentProcessed(ctx context.Context, pool *pg.Pool, logger *slog.Logger, msgID string, value []byte) error {
	var evt ordersv1.PaymentProcessed
	if err := proto.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("projection: unmarshal PaymentProcessed: %w", err)
	}

	orderID, err := uuid.Parse(evt.GetOrderId())
	if err != nil {
		return fmt.Errorf("projection: parse order_id %q: %w", evt.GetOrderId(), err)
	}

	_, err = inbox.ProcessOnce(ctx, pool, consumerGroup, msgID, func(ctx context.Context) error {
		q := storegen.New(pg.FromContext(ctx, pool))
		return q.MarkPaid(ctx, orderID)
	})
	if err != nil {
		return fmt.Errorf("projection: process PaymentProcessed: %w", err)
	}
	logger.Debug("projection: marked paid", "order_id", orderID)
	return nil
}
