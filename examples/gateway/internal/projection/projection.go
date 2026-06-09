// Package projection implements the gateway read-model projection consumer.
// It consumes from "orders.events" and "payments.events", deduplicating via
// the inbox, and upserts/updates the orders_read table.
package projection

import (
	"context"
	"fmt"
	"log/slog"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	storegen "go-boilerplate/examples/gateway/internal/store/gen"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

const consumerGroup = "gateway-projection"

// Versioned event types consumed by the projection.
const (
	OrderCreatedEventType         = "orders.OrderCreated.v1"
	PaymentProcessedEventType     = "orders.PaymentProcessed.v1"
	PaymentFailedEventType        = "orders.PaymentFailed.v1"
	OrderPaymentTimedOutEventType = "orders.OrderPaymentTimedOut.v1"
)

// NewHandler returns a kafka.HandlerFunc that handles both "orders.events"
// and "payments.events" records, routing on the versioned "event-type"
// header via consume.Typed (which also supplies inbox dedup, the uniform
// message-id policy, and principal-header extraction). Unknown event types
// are skipped — forward compatible.
//
// cache may be nil (gateway started without Redis). When non-nil, every
// successful projection write busts the order-view cache entry AFTER the
// inbox transaction commits, so readers on every instance see the new state
// instead of a stale cached view.
func NewHandler(pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache, opts ...consume.Option) kafka.HandlerFunc {
	opts = append([]consume.Option{consume.WithLogger(logger)}, opts...)
	return consume.New(pool, consumerGroup, opts...).Handler(
		consume.Typed(OrderCreatedEventType,
			func(ctx context.Context, evt *ordersv1.OrderCreated) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storegen.New(pg.FromContext(ctx, pool))
				if err := q.UpsertOrderCreated(ctx, storegen.UpsertOrderCreatedParams{
					OrderID:     orderID,
					CustomerID:  evt.GetCustomerId(),
					AmountCents: evt.GetAmountCents(),
					Currency:    evt.GetCurrency(),
				}); err != nil {
					return fmt.Errorf("projection: upsert OrderCreated: %w", err)
				}
				logger.Debug("projection: upserted OrderCreated", "order_id", orderID)
				return nil
			},
			func(ctx context.Context, evt *ordersv1.OrderCreated) {
				bustOrderCache(ctx, cache, logger, evt.GetOrderId())
			},
		),
		consume.Typed(PaymentProcessedEventType,
			func(ctx context.Context, evt *ordersv1.PaymentProcessed) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storegen.New(pg.FromContext(ctx, pool))
				rows, err := q.MarkPaid(ctx, orderID)
				if err != nil {
					return fmt.Errorf("projection: mark paid: %w", err)
				}
				if rows == 0 {
					// First terminal state wins: the row is already in a
					// terminal status (payment_failed/payment_timeout/paid) —
					// a later conflicting terminal event is ignored, loudly.
					logger.Warn("projection: PaymentProcessed ignored — order already in a terminal status",
						"order_id", orderID)
					return nil
				}
				logger.Debug("projection: marked paid", "order_id", orderID)
				return nil
			},
			func(ctx context.Context, evt *ordersv1.PaymentProcessed) {
				bustOrderCache(ctx, cache, logger, evt.GetOrderId())
			},
		),
		consume.Typed(OrderPaymentTimedOutEventType,
			func(ctx context.Context, evt *ordersv1.OrderPaymentTimedOut) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storegen.New(pg.FromContext(ctx, pool))
				rows, err := q.MarkPaymentTimeout(ctx, orderID)
				if err != nil {
					return fmt.Errorf("projection: mark payment_timeout: %w", err)
				}
				if rows == 0 {
					logger.Warn("projection: OrderPaymentTimedOut ignored — order already in a terminal status",
						"order_id", orderID)
					return nil
				}
				logger.Debug("projection: marked payment_timeout", "order_id", orderID)
				return nil
			},
			func(ctx context.Context, evt *ordersv1.OrderPaymentTimedOut) {
				bustOrderCache(ctx, cache, logger, evt.GetOrderId())
			},
		),
		consume.Typed(PaymentFailedEventType,
			func(ctx context.Context, evt *ordersv1.PaymentFailed) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storegen.New(pg.FromContext(ctx, pool))
				rows, err := q.MarkPaymentFailed(ctx, orderID)
				if err != nil {
					return fmt.Errorf("projection: mark payment_failed: %w", err)
				}
				if rows == 0 {
					logger.Warn("projection: PaymentFailed ignored — order already in a terminal status",
						"order_id", orderID, "reason", evt.GetReason())
					return nil
				}
				logger.Debug("projection: marked payment_failed",
					"order_id", orderID, "reason", evt.GetReason())
				return nil
			},
			func(ctx context.Context, evt *ordersv1.PaymentFailed) {
				bustOrderCache(ctx, cache, logger, evt.GetOrderId())
			},
		),
	)
}

func parseOrderID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("projection: parse order_id %q: %w", raw, err)
	}
	return id, nil
}

// bustOrderCache deletes the cached order view after a successful projection
// write (cache-bust closes the write-stale loop). Best-effort: a failed bust
// degrades to TTL-bounded staleness and must not fail the consumer.
func bustOrderCache(ctx context.Context, cache cqrs.Cache, logger *slog.Logger, orderID string) {
	if cache == nil {
		return
	}
	if err := cache.Delete(ctx, gatewayapp.OrderCacheKey(orderID)); err != nil {
		logger.Warn("projection: cache bust failed", "order_id", orderID, "error", err)
	}
}
