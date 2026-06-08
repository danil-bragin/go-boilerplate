package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is the query surface shared by *pgxpool.Pool and pgx.Tx. sqlc-generated
// queriers accept this interface, so the same query code runs inside or
// outside a transaction depending on what FromContext returns.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txCtxKey struct{}

// FromContext returns the transaction bound to ctx by RunInTx, or the pool's
// writer when no transaction is active.
func FromContext(ctx context.Context, p *Pool) DBTX {
	if tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return p.Writer()
}

// RunInTx begins a transaction on the writer pool, stores it in the context,
// invokes fn, and commits on success or rolls back on error/panic. This is the
// single place a write transaction is opened.
func RunInTx(ctx context.Context, p *Pool, fn func(ctx context.Context) error) (err error) {
	tx, err := p.Writer().Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: begin tx: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback(ctx)
			panic(r)
		}
	}()

	if err := fn(context.WithValue(ctx, txCtxKey{}, tx)); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("pg: rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: commit: %w", err)
	}
	return nil
}
