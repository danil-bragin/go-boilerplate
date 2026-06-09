// Package app contains CQRS query handlers for the gateway service.
package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// ErrOrderNotFound is returned when the requested order does not exist in the
// read model.
var ErrOrderNotFound = errors.New("app: order not found")

// GetOrder is the query type for the GetOrder CQRS handler.
type GetOrder struct {
	OrderID string
}

// OrderCacheKey returns the cache key for an order view. The read path
// (Caching behavior) and the write path (projection cache-bust on upsert)
// MUST use this same helper, or invalidation silently misses.
//
// Keys follow the versioned convention "<svc>:v<N>:<entity>:<id>"
// (see docs/conventions.md). Bump the version segment whenever OrderView's
// shape changes so stale entries become unreachable instead of unmarshalling
// into the new shape.
func OrderCacheKey(orderID string) string { return "gw:v1:order:" + orderID }

// OrderView is the read-model view returned by the GetOrder handler.
// Field names match the OpenAPI spec (JSON tags).
type OrderView struct {
	OrderID     string `json:"order_id"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// GetOrderHandler returns a raw (undecorated) CQRS query handler that reads an
// order from the read-model projection table.
// The caller should wrap it with Decorate (Logging, Tracing, Metrics, Caching)
// before use.
func GetOrderHandler(pool *pg.Pool) cqrs.HandlerFunc[GetOrder, OrderView] {
	return func(ctx context.Context, q GetOrder) (OrderView, error) {
		id, err := uuid.Parse(q.OrderID)
		if err != nil {
			return OrderView{}, ErrOrderNotFound
		}

		queries := storegen.New(pg.FromContextRead(ctx, pool))
		row, err := queries.GetOrderView(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return OrderView{}, ErrOrderNotFound
			}
			return OrderView{}, fmt.Errorf("app: get order view: %w", err)
		}

		return OrderView{
			OrderID:     row.OrderID.String(),
			Status:      row.Status,
			AmountCents: row.AmountCents,
			Currency:    row.Currency,
		}, nil
	}
}

// DecorateGetOrderHandler applies the standard CQRS pipeline to the raw handler.
// When cache is non-nil, the Caching behavior is also applied (order views are
// cached for 30 s under the key "gw:v1:order:<orderID>").
// When cache is nil (Redis unavailable at startup), the handler is still
// decorated with Logging / Tracing / Metrics but without Caching.
func DecorateGetOrderHandler(raw cqrs.HandlerFunc[GetOrder, OrderView], cache cqrs.Cache) cqrs.HandlerFunc[GetOrder, OrderView] {
	behaviors := []cqrs.Behavior[GetOrder, OrderView]{
		cqrs.Logging[GetOrder, OrderView]("GetOrder"),
		cqrs.Tracing[GetOrder, OrderView]("GetOrder"),
		cqrs.Metrics[GetOrder, OrderView]("GetOrder"),
	}
	if cache != nil {
		behaviors = append(
			behaviors,
			cqrs.CachingJSON[GetOrder, OrderView](
				cache,
				func(q GetOrder) string { return OrderCacheKey(q.OrderID) },
				30*time.Second,
			),
		)
	}
	return cqrs.Decorate(raw, behaviors...)
}
