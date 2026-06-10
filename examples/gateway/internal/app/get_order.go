// Package app contains CQRS query handlers for the gateway service.
package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// ErrOrderNotFound is returned when the requested order does not exist in the
// read model. It is a coded apperr (GATEWAY_ORDER_NOT_FOUND → 404), so it
// flows through httpx.FromError at the edge without inline mapping;
// errors.Is against this sentinel matches by code.
var ErrOrderNotFound error = apperr.New(apperrs.CodeOrderNotFound)

// OrderNotFound returns ErrOrderNotFound parameterized with the order id
// (problem+json params.order_id — every message variable must be a param,
// per Google AIP-193).
func OrderNotFound(orderID string) error {
	return apperr.New(apperrs.CodeOrderNotFound).WithParam("order_id", orderID)
}

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
// into the new shape. (v2: CustomerID added for read-path ownership checks;
// v3: CreatedAt added for the created_at API field.)
func OrderCacheKey(orderID string) string { return "gw:v3:order:" + orderID }

// OrderView is the read-model view returned by the GetOrder handler.
// Field names match the OpenAPI spec (JSON tags). CustomerID is internal:
// the API layer uses it for the read-path ownership check (owner or admin)
// and does not expose it in responses.
//
// CreatedAt is ALWAYS UTC: the pg pool scans timestamptz with
// ScanLocation=UTC, and the API layer serializes it as RFC 3339 with the
// "Z" suffix (.UTC().Format(time.RFC3339)) regardless of any X-Timezone
// header — the display-only created_at_local field is derived at the edge.
type OrderView struct {
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	Status      string    `json:"status"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

// GetOrderHandler returns a raw (undecorated) CQRS query handler that reads an
// order from the read-model projection table.
// The caller should wrap it with Decorate (Tracing, Logging, Metrics, Caching)
// before use.
func GetOrderHandler(pool *pg.Pool) cqrs.HandlerFunc[GetOrder, OrderView] {
	return func(ctx context.Context, q GetOrder) (OrderView, error) {
		id, err := uuid.Parse(q.OrderID)
		if err != nil {
			return OrderView{}, OrderNotFound(q.OrderID)
		}

		queries := storegen.New(pg.FromContextRead(ctx, pool))
		row, err := queries.GetOrderView(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return OrderView{}, OrderNotFound(q.OrderID)
			}
			return OrderView{}, fmt.Errorf("app: get order view: %w", err)
		}

		return OrderView{
			OrderID:     row.OrderID.String(),
			CustomerID:  row.CustomerID,
			Status:      row.Status,
			AmountCents: row.AmountCents,
			Currency:    row.Currency,
			CreatedAt:   row.CreatedAt.Time,
		}, nil
	}
}

// DecorateGetOrderHandler applies the standard CQRS pipeline to the raw handler.
// When cache is non-nil, the Caching behavior is also applied (order views are
// cached for 30 s under the key "gw:v3:order:<orderID>").
// When cache is nil (Redis unavailable at startup), the handler is still
// decorated with the standard stack but without Caching.
func DecorateGetOrderHandler(raw cqrs.HandlerFunc[GetOrder, OrderView], cache cqrs.Cache) cqrs.HandlerFunc[GetOrder, OrderView] {
	p := cqrs.StandardPipeline[GetOrder, OrderView]("GetOrder")
	if cache != nil {
		p.WithCache(cache, func(q GetOrder) string { return OrderCacheKey(q.OrderID) }, 30*time.Second)
	}
	return p.Decorate(raw)
}
