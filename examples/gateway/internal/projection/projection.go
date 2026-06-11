// Package projection implements the gateway read-model projection consumer.
// It consumes from "orders.events" and "payments.events", deduplicating via
// the inbox, and upserts/updates the orders_read table.
package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	storegen "go-boilerplate/examples/gateway/internal/store/gen"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

const consumerGroup = "gateway-projection"

// Versioned event types consumed by the projection (derived from the proto
// messages via consume.EventTypeFor).
var (
	OrderCreatedEventType         = consume.EventTypeFor[*ordersv1.OrderCreated](1)
	PaymentProcessedEventType     = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	PaymentFailedEventType        = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
	OrderPaymentTimedOutEventType = consume.EventTypeFor[*ordersv1.OrderPaymentTimedOut](1)
)

// StatusNotifier is called AFTER a status write committed, with the order id
// whose status (possibly) changed — the gateway wires sse.Streamer.Notify
// here so SSE subscribers receive live updates. May be nil (no-op).
type StatusNotifier func(ctx context.Context, orderID string)

// Store is the narrow orders_read write surface the projection handlers use.
// *storegen.Queries implements it; fast-lane contract tests substitute a
// recording fake (see NewHandlerWithStore).
type Store interface {
	UpsertOrderCreated(ctx context.Context, arg storegen.UpsertOrderCreatedParams) error
	MarkPaid(ctx context.Context, orderID uuid.UUID) (storegen.MarkPaidRow, error)
	MarkPaymentFailed(ctx context.Context, orderID uuid.UUID) (storegen.MarkPaymentFailedRow, error)
	MarkPaymentTimeout(ctx context.Context, orderID uuid.UUID) (storegen.MarkPaymentTimeoutRow, error)
}

// StoreFor resolves the Store for the current ctx. In production it binds the
// sqlc queries to the ambient inbox transaction (pg.FromContext); the
// indirection exists so contract tests can observe the exact field mapping
// the handlers write, without a database.
type StoreFor func(ctx context.Context) Store

// NewHandler returns a kafka.HandlerFunc that handles both "orders.events"
// and "payments.events" records, routing on the versioned "event-type"
// header via consume.Typed (which also supplies inbox dedup, the uniform
// message-id policy, and principal-header extraction). Unknown event types
// are skipped — forward compatible.
//
// cache may be nil (gateway started without Redis). When non-nil, every
// successful projection write busts the order-view cache entry AFTER the
// inbox transaction commits, so readers on every instance see the new state
// instead of a stale cached view. notify (may be nil) runs in the same
// post-commit hook and pushes the change to SSE subscribers.
func NewHandler(pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache, notify StatusNotifier, opts ...consume.Option) kafka.HandlerFunc {
	return NewHandlerWithStore(pool, func(ctx context.Context) Store {
		return storegen.New(pg.FromContext(ctx, pool))
	}, logger, cache, notify, opts...)
}

// NewHandlerWithStore is NewHandler with the orders_read write surface
// injected. It exists for fast-lane contract tests (recording Store, nil
// pool, consume.WithoutInbox); production wiring goes through NewHandler so
// writes always join the ambient inbox transaction.
func NewHandlerWithStore(pool *pg.Pool, storeFor StoreFor, logger *slog.Logger, cache cqrs.Cache, notify StatusNotifier, opts ...consume.Option) kafka.HandlerFunc {
	committed := func(ctx context.Context, orderID string) {
		bustOrderCache(ctx, cache, logger, orderID)
		if notify != nil {
			notify(ctx, orderID)
		}
	}
	metrics := newLifecycleMetrics()
	opts = append([]consume.Option{consume.WithLogger(logger)}, opts...)
	return consume.New(pool, consumerGroup, opts...).Handler(
		consume.TypedFor(
			1,
			func(ctx context.Context, evt *ordersv1.OrderCreated) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storeFor(ctx)
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
			consume.OnCommitted(func(ctx context.Context, evt *ordersv1.OrderCreated) {
				committed(ctx, evt.GetOrderId())
			}),
		),
		consume.TypedFor(
			1,
			func(ctx context.Context, evt *ordersv1.PaymentProcessed) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storeFor(ctx)
				row, err := q.MarkPaid(ctx, orderID)
				if errors.Is(err, pgx.ErrNoRows) {
					// First terminal state wins: the row is already in a
					// terminal status (payment_failed/payment_timeout/paid) —
					// a later conflicting terminal event is ignored, loudly.
					logger.Warn("projection: PaymentProcessed ignored — order already in a terminal status",
						"order_id", orderID)
					return nil
				}
				if err != nil {
					return fmt.Errorf("projection: mark paid: %w", err)
				}
				if !row.Inserted {
					// Inserted = the terminal event arrived before OrderCreated
					// and the upsert created a placeholder row: its creation
					// time is unknown, the ≈0s sample would lie compliant and
					// bias the SLO-2 good leg — skip the observation.
					metrics.observe(ctx, "paid", row.CreatedAt.Time)
				}
				logger.Debug("projection: marked paid", "order_id", orderID)
				return nil
			},
			consume.OnCommitted(func(ctx context.Context, evt *ordersv1.PaymentProcessed) {
				committed(ctx, evt.GetOrderId())
			}),
		),
		consume.TypedFor(
			1,
			func(ctx context.Context, evt *ordersv1.OrderPaymentTimedOut) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storeFor(ctx)
				row, err := q.MarkPaymentTimeout(ctx, orderID)
				if errors.Is(err, pgx.ErrNoRows) {
					logger.Warn("projection: OrderPaymentTimedOut ignored — order already in a terminal status",
						"order_id", orderID)
					return nil
				}
				if err != nil {
					return fmt.Errorf("projection: mark payment_timeout: %w", err)
				}
				if !row.Inserted {
					// Placeholder row (terminal-before-created reorder):
					// creation time unknown — skip, see the MarkPaid branch.
					metrics.observe(ctx, "payment_timeout", row.CreatedAt.Time)
				}
				logger.Debug("projection: marked payment_timeout", "order_id", orderID)
				return nil
			},
			consume.OnCommitted(func(ctx context.Context, evt *ordersv1.OrderPaymentTimedOut) {
				committed(ctx, evt.GetOrderId())
			}),
		),
		consume.TypedFor(
			1,
			func(ctx context.Context, evt *ordersv1.PaymentFailed) error {
				orderID, err := parseOrderID(evt.GetOrderId())
				if err != nil {
					return err
				}
				q := storeFor(ctx)
				row, err := q.MarkPaymentFailed(ctx, orderID)
				if errors.Is(err, pgx.ErrNoRows) {
					logger.Warn("projection: PaymentFailed ignored — order already in a terminal status",
						"order_id", orderID, "reason", evt.GetReason())
					return nil
				}
				if err != nil {
					return fmt.Errorf("projection: mark payment_failed: %w", err)
				}
				if !row.Inserted {
					// Placeholder row (terminal-before-created reorder):
					// creation time unknown — skip, see the MarkPaid branch.
					metrics.observe(ctx, "payment_failed", row.CreatedAt.Time)
				}
				logger.Debug("projection: marked payment_failed",
					"order_id", orderID, "reason", evt.GetReason())
				return nil
			},
			consume.OnCommitted(func(ctx context.Context, evt *ordersv1.PaymentFailed) {
				committed(ctx, evt.GetOrderId())
			}),
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
