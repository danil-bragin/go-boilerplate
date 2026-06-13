package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// ErrInvalidCursor is returned when a pagination cursor cannot be decoded.
// It is a coded apperr (GATEWAY_INVALID_CURSOR → 400): the edge maps it via
// httpx.FromError; errors.Is against this sentinel matches by code. The
// decode cause stays wrapped in the chain for logs but is never echoed to
// clients (FromError renders only the registered message).
var ErrInvalidCursor error = apperr.New(apperrs.CodeInvalidCursor)

// List pagination bounds (documented in openapi.yaml).
const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// ListOrders is the query type for the ListOrders CQRS handler.
type ListOrders struct {
	// Cursor is the opaque keyset cursor from the previous page ("" = first page).
	Cursor string
	// Limit is the page size; 0 means DefaultPageLimit, capped at MaxPageLimit.
	Limit int
	// CustomerID scopes the listing to one customer's rows ("" = all rows).
	// The API layer sets it to the principal's subject for non-admin
	// principals (read-path ownership); admins list unscoped.
	CustomerID string
}

// OrderPage is one cursor-paginated page of order views, newest first.
type OrderPage struct {
	Items []OrderView `json:"items"`
	// NextCursor is empty when the listing is exhausted.
	NextCursor string `json:"next_cursor,omitempty"`
}

// encodeCursor packs the keyset position (created_at, order_id) of the last
// row of a page into an opaque URL-safe string.
func encodeCursor(createdAt time.Time, orderID uuid.UUID) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + orderID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor. Returns ErrInvalidCursor on any
// malformed input — the cursor is opaque, clients must not construct it.
func decodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	orderID, err := uuid.Parse(id)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	return createdAt, orderID, nil
}

// listRow is the common shape of ListOrdersRow and ListOrdersByCustomerRow
// (identical columns, distinct sqlc-generated types).
type listRow struct {
	OrderID     uuid.UUID
	CustomerID  string
	AmountCents int64
	Currency    string
	Status      string
	CreatedAt   pgtype.Timestamptz
	UpdatedAt   pgtype.Timestamptz
}

// ListOrdersHandler returns a raw (undecorated) CQRS query handler that pages
// through the read-model projection with keyset pagination over
// (created_at, order_id) descending.
func ListOrdersHandler(sp *pg.ShardedPool) cqrs.HandlerFunc[ListOrders, OrderPage] {
	return func(ctx context.Context, q ListOrders) (OrderPage, error) {
		limit := q.Limit
		switch {
		case limit <= 0:
			limit = DefaultPageLimit
		case limit > MaxPageLimit:
			limit = MaxPageLimit
		}

		// First page: an infinite upper bound makes every row qualify in the
		// (created_at, order_id) < (cursor) keyset predicate.
		cursorAt := pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
		cursorID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
		if q.Cursor != "" {
			at, id, err := decodeCursor(q.Cursor)
			if err != nil {
				return OrderPage{}, err
			}
			cursorAt = pgtype.Timestamptz{Time: at, Valid: true}
			cursorID = id
		}

		// C2 wires LIST through the ShardedPool seam (single-shard read of shard
		// 0); C3 replaces this body with the scatter-gather k-way merge.
		queries := storegen.New(pg.FromContextRead(ctx, sp.Shards()[0]))
		var rows []listRow
		if q.CustomerID != "" {
			scoped, err := queries.ListOrdersByCustomer(ctx, storegen.ListOrdersByCustomerParams{
				CustomerID:      q.CustomerID,
				CursorCreatedAt: cursorAt,
				CursorOrderID:   cursorID,
				PageLimit:       int32(limit), //nolint:gosec // bounded by MaxPageLimit
			})
			if err != nil {
				return OrderPage{}, fmt.Errorf("app: list orders by customer: %w", err)
			}
			rows = make([]listRow, len(scoped))
			for i, r := range scoped {
				rows[i] = listRow(r)
			}
		} else {
			all, err := queries.ListOrders(ctx, storegen.ListOrdersParams{
				CursorCreatedAt: cursorAt,
				CursorOrderID:   cursorID,
				PageLimit:       int32(limit), //nolint:gosec // bounded by MaxPageLimit
			})
			if err != nil {
				return OrderPage{}, fmt.Errorf("app: list orders: %w", err)
			}
			rows = make([]listRow, len(all))
			for i, r := range all {
				rows[i] = listRow(r)
			}
		}

		page := OrderPage{Items: make([]OrderView, len(rows))}
		for i, row := range rows {
			page.Items[i] = OrderView{
				OrderID:     row.OrderID.String(),
				CustomerID:  row.CustomerID,
				Status:      row.Status,
				AmountCents: row.AmountCents,
				Currency:    row.Currency,
				CreatedAt:   row.CreatedAt.Time,
			}
		}
		// A full page may have more results: hand out the keyset position of
		// the last row. A short page is definitively the end.
		if len(rows) == limit {
			last := rows[len(rows)-1]
			page.NextCursor = encodeCursor(last.CreatedAt.Time, last.OrderID)
		}
		return page, nil
	}
}

// DecorateListOrdersHandler applies the standard CQRS pipeline (no caching:
// list pages are cheap keyset scans and cache invalidation for ranges is not
// worth the staleness risk).
func DecorateListOrdersHandler(raw cqrs.HandlerFunc[ListOrders, OrderPage]) cqrs.HandlerFunc[ListOrders, OrderPage] {
	return cqrs.StandardPipeline[ListOrders, OrderPage]("ListOrders").Decorate(raw)
}
