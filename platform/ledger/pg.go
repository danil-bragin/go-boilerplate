package ledger

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PgStore is the Postgres-backed Store over the platform/ledger schema
// (Migrations). It resolves its query surface from the context via
// pg.FromContext, so it joins the caller's ambient transaction.
//
// # Atomicity invariant
//
// Post issues several statements (the transaction row + one row per entry) that
// MUST commit together. Call it inside a caller-owned transaction (a
// cqrs.Transaction-decorated command, the inbox.ProcessOnce ambient tx, or an
// explicit pg.RunInTx). With NO ambient transaction, pg.FromContext falls back
// to the writer pool and each statement auto-commits independently — a partial
// posting could then persist. Enforced by convention, exactly like the example
// repositories; see the pg.FromContext godoc.
type PgStore struct {
	pool *pg.Pool
}

// NewPgStore builds a Postgres ledger store over pool.
func NewPgStore(pool *pg.Pool) *PgStore {
	return &PgStore{pool: pool}
}

func (s *PgStore) db(ctx context.Context) pg.DBTX {
	return pg.FromContext(ctx, s.pool)
}

// RegisterAccount creates an account (idempotently — re-registering the same id
// is a no-op). The asset must be a registered money asset.
func (s *PgStore) RegisterAccount(ctx context.Context, a Account) error {
	if _, ok := money.Lookup(a.Asset); !ok {
		return fmt.Errorf("ledger: account %s: unknown asset %q", a.ID, a.Asset)
	}
	_, err := s.db(ctx).Exec(ctx,
		`insert into ledger_accounts (id, asset, normal) values ($1, $2, $3)
		 on conflict (id) do nothing`,
		a.ID, a.Asset, a.Normal.String())
	if err != nil {
		return fmt.Errorf("ledger: register account %s: %w", a.ID, err)
	}
	return nil
}

// Post validates and records a balanced posting (see the atomicity note on the
// type). Re-posting an already-seen IdempotencyKey is a no-op: the unique key
// on ledger_transactions collapses the duplicate, so no entries are written the
// second time (safe under at-least-once delivery and concurrent posters).
func (s *PgStore) Post(ctx context.Context, p Posting) error {
	if err := p.Validate(); err != nil {
		return err
	}
	db := s.db(ctx)
	txID := uuid.New()
	tag, err := db.Exec(ctx,
		`insert into ledger_transactions (id, idempotency_key) values ($1, $2)
		 on conflict (idempotency_key) do nothing`,
		txID, p.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("ledger: insert transaction: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // idempotent: this key was already posted
	}
	for _, e := range p.Entries {
		var asset string
		err := db.QueryRow(ctx, `select asset from ledger_accounts where id = $1`, e.AccountID).Scan(&asset)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrUnknownAccount, e.AccountID)
		}
		if err != nil {
			return fmt.Errorf("ledger: lookup account %s: %w", e.AccountID, err)
		}
		if asset != e.Amount.Asset() {
			return fmt.Errorf("%w: account %s is %s, entry is %s", ErrAccountAssetMismatch, e.AccountID, asset, e.Amount.Asset())
		}
		amountVal, err := e.Amount.AmountValue().Value()
		if err != nil {
			return fmt.Errorf("ledger: entry amount value: %w", err)
		}
		amount, _ := amountVal.(string)
		if _, err := db.Exec(ctx,
			`insert into ledger_entries (tx_id, account_id, direction, amount, asset)
			 values ($1, $2, $3, $4, $5)`,
			txID, e.AccountID, e.Direction.String(), amount, e.Amount.Asset()); err != nil {
			return fmt.Errorf("ledger: insert entry: %w", err)
		}
	}
	return nil
}

// Balance returns the account's derived balance, computed in SQL as the signed
// sum of its entries (normal side positive, opposite negative).
func (s *PgStore) Balance(ctx context.Context, accountID string) (money.Money, error) {
	db := s.db(ctx)
	var asset, normal string
	err := db.QueryRow(ctx, `select asset, normal from ledger_accounts where id = $1`, accountID).Scan(&asset, &normal)
	if errors.Is(err, pgx.ErrNoRows) {
		return money.Money{}, fmt.Errorf("%w: %s", ErrUnknownAccount, accountID)
	}
	if err != nil {
		return money.Money{}, fmt.Errorf("ledger: lookup account %s: %w", accountID, err)
	}
	var sum string
	err = db.QueryRow(ctx,
		`select coalesce(sum(case when direction = $2 then amount else -amount end), 0)::text
		 from ledger_entries where account_id = $1`,
		accountID, normal).Scan(&sum)
	if err != nil {
		return money.Money{}, fmt.Errorf("ledger: balance %s: %w", accountID, err)
	}
	return money.ScanRow(sum, asset)
}

// compile-time conformance.
var _ Store = (*PgStore)(nil)
