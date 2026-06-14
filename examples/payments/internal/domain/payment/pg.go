package payment

import (
	"context"
	"fmt"

	"go-boilerplate/examples/payments/internal/store/gen"
	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg"
)

// PgRepository is the Postgres Repository over the sqlc-generated queries.
//
// # Ambient-transaction invariant
//
// Every method resolves its query surface via pg.FromContext(ctx), so the
// SAME repository works unchanged under all three transaction owners: the
// inbox.ProcessOnce ambient transaction (the production consumer path), a
// cqrs.Transaction-decorated handler, and an explicit pg.RunInTx. Inside any
// of those, writes join the caller's transaction and commit/roll back with
// it — which is what makes "payment row + outbox event commit together"
// hold.
//
// Writer-fallback hazard: with NO transaction in ctx, pg.FromContext falls
// back to the writer pool and each statement auto-commits independently —
// atomicity silently disappears. Enforced by convention (every command path
// runs under one of the owners above); see the pg.FromContext godoc.
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

// Insert writes a new payment row; created_at is DB time (DEFAULT now()). The
// amount goes into the NUMERIC column via money's driver.Valuer (its canonical
// decimal string) and the asset into the text column — lossless, no float.
func (r *PgRepository) Insert(ctx context.Context, p Payment) error {
	amountVal, err := p.Amount.AmountValue().Value()
	if err != nil {
		return fmt.Errorf("payment: amount value: %w", err)
	}
	amount, _ := amountVal.(string)
	return r.q(ctx).InsertPayment(ctx, gen.InsertPaymentParams{
		ID:      p.ID,
		OrderID: p.OrderID,
		Amount:  amount,
		Asset:   p.Amount.Asset(),
		Status:  p.Status,
	})
}

// GetByOrder returns the payment recorded for an order. The NUMERIC amount and
// asset columns are reassembled into a money.Money via ScanRow.
func (r *PgRepository) GetByOrder(ctx context.Context, orderID string) (Payment, error) {
	row, err := r.q(ctx).GetPaymentByOrder(ctx, orderID)
	if err != nil {
		return Payment{}, fmt.Errorf("payment: get by order %s: %w", orderID, err)
	}
	amount, err := money.ScanRow(row.Amount, row.Asset)
	if err != nil {
		return Payment{}, fmt.Errorf("payment: get by order %s: %w", orderID, err)
	}
	return Payment{
		ID:        row.ID,
		OrderID:   row.OrderID,
		Amount:    amount,
		Status:    row.Status,
		CreatedAt: row.CreatedAt.Time,
	}, nil
}

// compile-time conformance: the consumer-side port is satisfied.
var _ Repository = (*PgRepository)(nil)
