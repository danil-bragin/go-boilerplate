package order

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/examples/orders/internal/store/gen"
	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
)

// PgRepository is the Postgres Repository over the sqlc-generated queries.
//
// # Ambient-transaction invariant
//
// Every method resolves its query surface via pg.FromContext(ctx), so the
// SAME repository works unchanged under all three transaction owners in this
// repo: the inbox.ProcessOnce ambient transaction (Kafka consumers), a
// cqrs.Transaction-decorated handler, and an explicit pg.RunInTx (the unpaid
// watcher). Inside any of those, repository writes join the caller's
// transaction and commit/roll back with it — which is what makes
// "order row + outbox event commit together" hold without the repository
// knowing who owns the transaction.
//
// Writer-fallback hazard: when ctx carries NO transaction, pg.FromContext
// silently falls back to the writer pool and each statement auto-commits
// independently — atomicity quietly disappears rather than failing loudly.
// The invariant is enforced by convention (every command path runs under one
// of the three owners above) and pinned by the integration tests; see the
// pg.FromContext godoc for the full design note.
type PgRepository struct {
	pool *pg.Pool
}

// NewPgRepository builds the Postgres repository over pool.
func NewPgRepository(pool *pg.Pool) *PgRepository {
	return &PgRepository{pool: pool}
}

// q returns the sqlc querier bound to the ambient transaction (or the writer
// pool outside one — see the type-level hazard note).
func (r *PgRepository) q(ctx context.Context) *gen.Queries {
	return gen.New(pg.FromContext(ctx, r.pool))
}

// Insert writes a new order row; created_at is DB time (DEFAULT now()). The
// amount goes into the NUMERIC column via money's driver.Valuer (its canonical
// decimal string) and the asset into the text column — lossless, no float.
func (r *PgRepository) Insert(ctx context.Context, o Order) error {
	amountVal, err := o.Amount.AmountValue().Value()
	if err != nil {
		return fmt.Errorf("order: amount value: %w", err)
	}
	amount, _ := amountVal.(string)
	return r.q(ctx).InsertOrder(ctx, gen.InsertOrderParams{
		ID:         o.ID,
		CustomerID: o.CustomerID,
		Amount:     amount,
		Asset:      o.Amount.Asset(),
		Status:     string(o.Status),
	})
}

// Get returns the order by id. The NUMERIC amount and asset columns are
// reassembled into a money.Money via ScanRow.
func (r *PgRepository) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	row, err := r.q(ctx).GetOrder(ctx, id)
	if err != nil {
		return Order{}, fmt.Errorf("order: get %s: %w", id, err)
	}
	amount, err := money.ScanRow(row.Amount, row.Asset)
	if err != nil {
		return Order{}, fmt.Errorf("order: get %s: %w", id, err)
	}
	return Order{
		ID:         row.ID,
		CustomerID: row.CustomerID,
		Amount:     amount,
		Status:     Status(row.Status),
		CreatedAt:  row.CreatedAt.Time,
	}, nil
}

// MarkPaymentOutcome applies the guarded terminal-outcome UPDATE
// (only from 'created'); false when the guard matched no row.
func (r *PgRepository) MarkPaymentOutcome(ctx context.Context, id uuid.UUID, to Status) (bool, error) {
	rows, err := r.q(ctx).MarkOrderPaymentOutcome(ctx, gen.MarkOrderPaymentOutcomeParams{
		ID:     id,
		Status: string(to),
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// MarkTimeoutEmitted runs the compare-and-set claim (flag + status flip in
// one statement); false when another claimer won.
func (r *PgRepository) MarkTimeoutEmitted(ctx context.Context, id uuid.UUID) (bool, error) {
	rows, err := r.q(ctx).MarkPaymentTimeoutEmitted(ctx, id)
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// ListUnpaidExpired selects expiry candidates with the cutoff computed
// against the DATABASE clock (now() in SQL) — see Repository.ListUnpaidExpired
// for why an injected application clock is deliberately not used here.
func (r *PgRepository) ListUnpaidExpired(ctx context.Context, unpaidFor time.Duration, limit int32) ([]UnpaidOrder, error) {
	rows, err := r.q(ctx).ListUnpaidExpired(ctx, gen.ListUnpaidExpiredParams{
		DeadlineSeconds: unpaidFor.Seconds(),
		BatchLimit:      limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]UnpaidOrder, 0, len(rows))
	for _, row := range rows {
		out = append(out, UnpaidOrder{ID: row.ID, CreatedAt: row.CreatedAt.Time})
	}
	return out, nil
}

// compile-time conformance: the consumer-side port is satisfied.
var _ Repository = (*PgRepository)(nil)
