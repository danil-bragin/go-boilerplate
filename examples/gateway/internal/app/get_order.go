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
// into the new shape. (v2: CustomerID added for read-path ownership checks.)
func OrderCacheKey(orderID string) string { return "gw:v2:order:" + orderID }

// OrderView is the read-model view returned by the GetOrder handler.
// Field names match the OpenAPI spec (JSON tags). CustomerID is internal:
// the API layer uses it for the read-path ownership check (owner or admin)
// and does not expose it in responses.
type OrderView struct {
	OrderID     string `json:"order_id"`
	CustomerID  string `json:"customer_id"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// GetOrderHandler returns a raw (undecorated) CQRS query handler that reads an
// order from the read-model projection table.
// The caller should wrap it with Decorate (Tracing, Logging, Metrics, Caching)
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
			CustomerID:  row.CustomerID,
			Status:      row.Status,
			AmountCents: row.AmountCents,
			Currency:    row.Currency,
		}, nil
	}
}

// DecorateGetOrderHandler applies the standard CQRS pipeline to the raw handler.
// When cache is non-nil, the Caching behavior is also applied (order views are
// cached for 30 s under the key "gw:v2:order:<orderID>").
// When cache is nil (Redis unavailable at startup), the handler is still
// decorated with the standard stack but without Caching.
func DecorateGetOrderHandler(raw cqrs.HandlerFunc[GetOrder, OrderView], cache cqrs.Cache) cqrs.HandlerFunc[GetOrder, OrderView] {
	p := cqrs.StandardPipeline[GetOrder, OrderView]("GetOrder")
	if cache != nil {
		p.WithCache(cache, func(q GetOrder) string { return OrderCacheKey(q.OrderID) }, 30*time.Second)
	}
	return p.Decorate(raw)
}
