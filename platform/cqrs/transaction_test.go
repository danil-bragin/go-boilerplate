package cqrs_test

import (
	"context"
	"errors"
	"testing"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// setupTxCounter creates a fresh Postgres pool with a counter(n int) table
// seeded with a single row of 0.
func setupTxCounter(t *testing.T) *pg.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	_, err = pool.Writer().Exec(ctx, `create table counter (n int not null)`)
	require.NoError(t, err)
	_, err = pool.Writer().Exec(ctx, `insert into counter(n) values (0)`)
	require.NoError(t, err)
	return pool
}

func readTxCounter(t *testing.T, pool *pg.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(), `select n from counter`).Scan(&n))
	return n
}

type (
	txCmd    struct{}
	txResult struct{ N int }
)

// TestTransaction_CommitsOnSuccess verifies that a command handler wrapped with
// Transaction commits its writes and returns the result on success.
func TestTransaction_CommitsOnSuccess(t *testing.T) {
	pool := setupTxCounter(t)
	ctx := context.Background()

	handler := cqrs.HandlerFunc[txCmd, txResult](func(ctx context.Context, _ txCmd) (txResult, error) {
		_, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 1`)
		if err != nil {
			return txResult{}, err
		}
		return txResult{N: 1}, nil
	})

	decorated := cqrs.Decorate(handler, cqrs.Transaction[txCmd, txResult](pool))
	res, err := decorated(ctx, txCmd{})

	require.NoError(t, err)
	require.Equal(t, txResult{N: 1}, res)
	require.Equal(t, 1, readTxCounter(t, pool))
}

// TestTransaction_RollsBackOnError verifies that when the command handler
// returns an error the transaction is rolled back, leaving the table unchanged.
func TestTransaction_RollsBackOnError(t *testing.T) {
	pool := setupTxCounter(t)
	ctx := context.Background()

	boom := errors.New("boom")
	handler := cqrs.HandlerFunc[txCmd, txResult](func(ctx context.Context, _ txCmd) (txResult, error) {
		_, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 1`)
		if err != nil {
			return txResult{}, err
		}
		return txResult{}, boom
	})

	decorated := cqrs.Decorate(handler, cqrs.Transaction[txCmd, txResult](pool))
	res, err := decorated(ctx, txCmd{})

	require.ErrorIs(t, err, boom)
	require.Equal(t, txResult{}, res, "zero result expected on error")
	require.Equal(t, 0, readTxCounter(t, pool), "counter must be rolled back")
}
