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

// multiCursorVersion prefixes the M>1 opaque cursor payload so it can never be
// mistaken for the M=1 "RFC3339|uuid" single-position cursor (which starts with
// a digit). The byte is part of the opaque blob — clients must not construct it.
const multiCursorVersion = "\x01m"

// posStart is the sentinel for a shard whose position is the first-page
// infinite upper bound (no rows emitted from it yet).
const posStart = "*"

// encodeMultiCursor packs one (created_at, order_id) position per physical
// shard into an opaque URL-safe string: a version marker, then one line per
// shard ("*" = first-page start, else "RFC3339Nano|uuid"). The position count
// is the shard count, so a decode against a different M is rejected.
func encodeMultiCursor(positions []shardPos) string {
	parts := make([]string, len(positions))
	for i, p := range positions {
		if p.at.InfinityModifier == pgtype.Infinity {
			parts[i] = posStart
			continue
		}
		parts[i] = p.at.Time.UTC().Format(time.RFC3339Nano) + "|" + p.id.String()
	}
	raw := multiCursorVersion + strings.Join(parts, "\n")
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeMultiCursor reverses encodeMultiCursor for an M=n shard set. An empty
// cursor yields n first-page start positions. Any malformed input — bad base64,
// missing version marker, wrong shard count, or an unparseable line — maps to
// ErrInvalidCursor (the cursor is opaque and client-supplied, so garbage is
// expected traffic and must be a 400, never a 500).
func decodeMultiCursor(cursor string, n int) ([]shardPos, error) {
	positions := make([]shardPos, n)
	if cursor == "" {
		for i := range positions {
			positions[i] = startPos()
		}
		return positions, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	s := string(raw)
	if !strings.HasPrefix(s, multiCursorVersion) {
		return nil, ErrInvalidCursor
	}
	parts := strings.Split(strings.TrimPrefix(s, multiCursorVersion), "\n")
	if len(parts) != n {
		// Shard count changed (rescale) or a forged cursor — fail closed.
		return nil, ErrInvalidCursor
	}
	for i, part := range parts {
		if part == posStart {
			positions[i] = startPos()
			continue
		}
		ts, id, ok := strings.Cut(part, "|")
		if !ok {
			return nil, ErrInvalidCursor
		}
		at, perr := time.Parse(time.RFC3339Nano, ts)
		if perr != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCursor, perr)
		}
		oid, perr := uuid.Parse(id)
		if perr != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCursor, perr)
		}
		positions[i] = shardPos{at: pgtype.Timestamptz{Time: at, Valid: true}, id: oid}
	}
	return positions, nil
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

// shardPos is one physical shard's keyset position (the exclusive
// (created_at, order_id) upper bound for that shard's next-page query).
type shardPos struct {
	at pgtype.Timestamptz
	id uuid.UUID
}

// startPos is the first-page position: an infinite upper bound so every row
// qualifies in the (created_at, order_id) < (cursor) keyset predicate.
func startPos() shardPos {
	return shardPos{
		at: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true},
		id: uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
	}
}

// clampLimit applies the documented page bounds.
func clampLimit(n int) int {
	switch {
	case n <= 0:
		return DefaultPageLimit
	case n > MaxPageLimit:
		return MaxPageLimit
	}
	return n
}

// rowBefore reports whether a sorts strictly before b under the listing order
// (created_at DESC, order_id DESC) — i.e. a should be emitted before b.
func rowBefore(aAt time.Time, aID uuid.UUID, bAt time.Time, bID uuid.UUID) bool {
	if !aAt.Equal(bAt) {
		return aAt.After(bAt)
	}
	// order_id DESC tiebreak; id is a globally-unique UUIDv5 → stable.
	return aID.String() > bID.String()
}

// ListOrdersHandler returns a raw (undecorated) CQRS query handler that pages
// through the read-model projection with keyset pagination over
// (created_at, order_id) descending.
//
// LIST is keyless w.r.t. the shard key (the global list spans every order id;
// the customer-scoped list filters by customer_id, which is independent of the
// order-id shard routing) — so it fans out across ALL shards and k-way merges
// the per-shard pages by (created_at, order_id). At M=1 (sp.Len()==1) it takes
// the single-shard fast path: the exact original query and the original opaque
// cursor bytes, byte-identical to the pre-sharding handler.
func ListOrdersHandler(sp *pg.ShardedPool) cqrs.HandlerFunc[ListOrders, OrderPage] {
	return func(ctx context.Context, q ListOrders) (OrderPage, error) {
		limit := clampLimit(q.Limit)
		if sp.Len() == 1 {
			return listSingleShard(ctx, sp.Shards()[0], q, limit)
		}
		return listScatterGather(ctx, sp, q, limit)
	}
}

// listSingleShard is the M=1 path: it preserves the original single-cursor
// codec (encodeCursor/decodeCursor) and the original keyset query exactly, so
// the wire cursor and the result are byte-identical to the pre-sharding
// handler.
func listSingleShard(ctx context.Context, pool *pg.Pool, q ListOrders, limit int) (OrderPage, error) {
	pos := startPos()
	if q.Cursor != "" {
		at, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return OrderPage{}, err
		}
		pos = shardPos{at: pgtype.Timestamptz{Time: at, Valid: true}, id: id}
	}

	rows, err := queryShardPage(ctx, pool, q.CustomerID, pos, limit)
	if err != nil {
		return OrderPage{}, err
	}

	page := OrderPage{Items: rowsToViews(rows)}
	// A full page may have more results: hand out the keyset position of the
	// last row. A short page is definitively the end.
	if len(rows) == limit {
		last := rows[len(rows)-1]
		page.NextCursor = encodeCursor(last.CreatedAt.Time, last.OrderID)
	}
	return page, nil
}

// listScatterGather is the M>1 path: it queries every shard for up to `limit`
// rows after that shard's own position (carried in a multi-position cursor),
// k-way merges the per-shard pages by (created_at, order_id) DESC, emits the
// globally-first `limit`, and encodes the next cursor as each shard's
// last-EMITTED position (a shard that emitted nothing keeps its incoming
// position so its rows are re-fetched and re-merged next page — bounded by
// `limit`, with no duplicates because the merge is deterministic and the
// per-shard cursor is exclusive).
func listScatterGather(ctx context.Context, sp *pg.ShardedPool, q ListOrders, limit int) (OrderPage, error) {
	n := sp.Len()
	positions, err := decodeMultiCursor(q.Cursor, n)
	if err != nil {
		return OrderPage{}, err
	}

	perShard := make([][]listRow, n)
	if ferr := sp.ForEachShard(ctx, func(idx int, p *pg.Pool) error {
		rows, qerr := queryShardPage(ctx, p, q.CustomerID, positions[idx], limit)
		if qerr != nil {
			return qerr
		}
		perShard[idx] = rows
		return nil
	}); ferr != nil {
		return OrderPage{}, fmt.Errorf("app: list orders fan-out: %w", ferr)
	}

	// K-way merge: cursor[i] is the next un-emitted index into perShard[i].
	cursors := make([]int, n)
	// nextPos starts at each shard's INCOMING position; it advances to a
	// shard's last-emitted row as rows are taken from that shard.
	nextPos := make([]shardPos, n)
	copy(nextPos, positions)

	emitted := make([]listRow, 0, limit)
	for len(emitted) < limit {
		best := -1
		for i := range perShard {
			if cursors[i] >= len(perShard[i]) {
				continue
			}
			cand := perShard[i][cursors[i]]
			if best == -1 {
				best = i
				continue
			}
			b := perShard[best][cursors[best]]
			if rowBefore(cand.CreatedAt.Time, cand.OrderID, b.CreatedAt.Time, b.OrderID) {
				best = i
			}
		}
		if best == -1 {
			break // every shard drained
		}
		row := perShard[best][cursors[best]]
		emitted = append(emitted, row)
		cursors[best]++
		nextPos[best] = shardPos{at: row.CreatedAt, id: row.OrderID}
	}

	page := OrderPage{Items: rowsToViews(emitted)}
	// A full page may have more results (some shard may have un-fetched or
	// un-emitted rows): hand out the per-shard positions. A short page means
	// every shard was drained within this page — definitively the end.
	if len(emitted) == limit {
		page.NextCursor = encodeMultiCursor(nextPos)
	}
	return page, nil
}

// rowsToViews maps store rows to the API read-model view shape.
func rowsToViews(rows []listRow) []OrderView {
	out := make([]OrderView, len(rows))
	for i, row := range rows {
		out[i] = OrderView{
			OrderID:     row.OrderID.String(),
			CustomerID:  row.CustomerID,
			Status:      row.Status,
			AmountCents: row.AmountCents,
			Currency:    row.Currency,
			CreatedAt:   row.CreatedAt.Time,
		}
	}
	return out
}

// queryShardPage runs the existing keyset query on one shard pool: the global
// ListOrders when customerID is empty, else the ownership-scoped
// ListOrdersByCustomer. It returns up to `limit` rows, newest first, after pos.
func queryShardPage(ctx context.Context, pool *pg.Pool, customerID string, pos shardPos, limit int) ([]listRow, error) {
	queries := storegen.New(pg.FromContextRead(ctx, pool))
	if customerID != "" {
		scoped, err := queries.ListOrdersByCustomer(ctx, storegen.ListOrdersByCustomerParams{
			CustomerID:      customerID,
			CursorCreatedAt: pos.at,
			CursorOrderID:   pos.id,
			PageLimit:       int32(limit), //nolint:gosec // bounded by MaxPageLimit
		})
		if err != nil {
			return nil, fmt.Errorf("app: list orders by customer: %w", err)
		}
		rows := make([]listRow, len(scoped))
		for i, r := range scoped {
			rows[i] = listRow(r)
		}
		return rows, nil
	}
	all, err := queries.ListOrders(ctx, storegen.ListOrdersParams{
		CursorCreatedAt: pos.at,
		CursorOrderID:   pos.id,
		PageLimit:       int32(limit), //nolint:gosec // bounded by MaxPageLimit
	})
	if err != nil {
		return nil, fmt.Errorf("app: list orders: %w", err)
	}
	rows := make([]listRow, len(all))
	for i, r := range all {
		rows[i] = listRow(r)
	}
	return rows, nil
}

// DecorateListOrdersHandler applies the standard CQRS pipeline (no caching:
// list pages are cheap keyset scans and cache invalidation for ranges is not
// worth the staleness risk).
func DecorateListOrdersHandler(raw cqrs.HandlerFunc[ListOrders, OrderPage]) cqrs.HandlerFunc[ListOrders, OrderPage] {
	return cqrs.StandardPipeline[ListOrders, OrderPage]("ListOrders").Decorate(raw)
}
