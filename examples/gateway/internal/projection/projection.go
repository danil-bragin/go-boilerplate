// Package projection implements the gateway read-model projection consumer.
// It consumes from "orders.events" and "payments.events", deduplicating via inbox,
// and upserts/updates the orders_read table.
package projection

import (
	"context"
	"fmt"
	"log/slog"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	storegen "go-boilerplate/examples/gateway/internal/store/gen"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

const consumerGroup = "gateway-projection"

// NewHandler returns a kafka.HandlerFunc that handles both "orders.events" and
// "payments.events" records, routing on the "event-type" header.
//
// cache may be nil (gateway started without Redis). When non-nil, every
// successful projection write busts the order-view cache entry so readers on
// every instance see the new state instead of a stale cached view.
func NewHandler(pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache) kafka.HandlerFunc {
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
			return handleOrderCreated(ctx, pool, logger, cache, msgID, r.Value)
		case "PaymentProcessed":
			return handlePaymentProcessed(ctx, pool, logger, cache, msgID, r.Value)
		default:
			// Unknown event type — skip silently (forward-compatible).
			logger.Debug("projection: skipping unknown event type", "event_type", eventType, "topic", r.Topic)
			return nil
		}
	}
}

// bustOrderCache deletes the cached order view after a successful projection
// write (cache-bust closes the write-stale loop). Best-effort: a failed bust
// degrades to TTL-bounded staleness and must not fail the consumer.
func bustOrderCache(ctx context.Context, cache cqrs.Cache, logger *slog.Logger, orderID uuid.UUID) {
	if cache == nil {
		return
	}
	if err := cache.Delete(ctx, gatewayapp.OrderCacheKey(orderID.String())); err != nil {
		logger.Warn("projection: cache bust failed", "order_id", orderID, "error", err)
	}
}

func handleOrderCreated(ctx context.Context, pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache, msgID string, value []byte) error {
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
	bustOrderCache(ctx, cache, logger, orderID)
	logger.Debug("projection: upserted OrderCreated", "order_id", orderID)
	return nil
}

func handlePaymentProcessed(ctx context.Context, pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache, msgID string, value []byte) error {
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
	bustOrderCache(ctx, cache, logger, orderID)
	logger.Debug("projection: marked paid", "order_id", orderID)
	return nil
}
