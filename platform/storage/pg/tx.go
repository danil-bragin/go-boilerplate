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
// WRITER when no transaction is active. Use this for write handlers and
// read-your-writes. For query-side handlers that may target a read replica,
// use FromContextRead instead.
//
// # Writer-fallback hazard (design note)
//
// The no-transaction fallback is deliberate — it lets the same repository
// code run inside AND outside RunInTx — but it is also a foot-gun: a command
// handler whose pipeline forgot the Transaction behavior does NOT fail; its
// writes silently execute as independent auto-commit statements on the writer
// pool. Atomicity (e.g. "domain write + outbox enqueue commit together") is
// then quietly lost. There is no reliable way to detect this here without
// banning legitimate non-transactional writes, so the invariant is enforced
// by convention: every COMMAND handler must be decorated with
// cqrs.Transaction / Pipeline.WithTransaction, and integration tests should
// assert outbox atomicity (see outbox tests for the pattern).
func FromContext(ctx context.Context, p *Pool) DBTX {
	if tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return p.Writer()
}

// FromContextRead returns the transaction bound to ctx (so reads inside a
// write tx observe their own writes), or the READER pool when no transaction
// is active. Use this for query-side handlers that may target a read replica.
func FromContextRead(ctx context.Context, p *Pool) DBTX {
	if tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return p.Reader()
}

// RunInTx runs fn inside a database transaction. On success it commits; on
// error or panic it rolls back.
//
// Nesting: if ctx already carries a transaction (set by an outer RunInTx),
// the inner call opens a SAVEPOINT on the same connection using pgx's nested
// Begin (pgx v5 implements sub-transactions as SAVEPOINTs). Committing the
// inner tx releases the savepoint; rolling back rolls back to it — standard
// nested-transaction semantics. The outer transaction is unaffected by an
// inner rollback.
func RunInTx(ctx context.Context, p *Pool, fn func(ctx context.Context) error) (err error) {
	var tx pgx.Tx
	if existing, ok := ctx.Value(txCtxKey{}).(pgx.Tx); ok && existing != nil {
		// Nested: open a savepoint on the same connection/transaction.
		tx, err = existing.Begin(ctx)
	} else {
		tx, err = p.Writer().Begin(ctx)
	}
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
