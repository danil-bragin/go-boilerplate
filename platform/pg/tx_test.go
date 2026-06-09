package pg_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"

	"github.com/stretchr/testify/require"
)

func setupCounter(t *testing.T) *pg.Pool {
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

func readCounter(t *testing.T, pool *pg.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(), `select n from counter`).Scan(&n))
	return n
}

func TestRunInTx_CommitsOnSuccess(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 1`)
		return err
	})
	require.NoError(t, err)

	var n int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select n from counter`).Scan(&n))
	require.Equal(t, 1, n)
}

func TestRunInTx_RollsBackOnError(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	wantErr := errors.New("boom")
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		if _, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 5`); err != nil {
			return err
		}
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)

	var n int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select n from counter`).Scan(&n))
	require.Equal(t, 0, n, "update must be rolled back")
}

func TestFromContext_FallsBackToWriterPoolWithoutTx(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()
	// No tx in context → FromContext returns the writer pool.
	_, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = 42`)
	require.NoError(t, err)
	var n int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select n from counter`).Scan(&n))
	require.Equal(t, 42, n)
}

// TestRunInTx_NestedRollbackInnerKeepsOuter verifies savepoint semantics:
// inner rollback only rolls back to the savepoint; the outer tx still commits.
func TestRunInTx_NestedRollbackInnerKeepsOuter(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	innerErr := errors.New("inner failure")
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		// Outer: +1
		if _, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 1`); err != nil {
			return err
		}
		// Inner: +10, then error → rolls back to savepoint only.
		innerRunErr := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			if _, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 10`); err != nil {
				return err
			}
			return innerErr
		})
		if !errors.Is(innerRunErr, innerErr) {
			return fmt.Errorf("expected inner error, got: %w", innerRunErr)
		}
		// Outer continues after inner rollback.
		return nil
	})
	require.NoError(t, err)

	// Inner +10 rolled back to savepoint; outer +1 committed.
	require.Equal(t, 1, readCounter(t, pool))
}

// TestRunInTx_NestedBothCommit verifies that when both outer and inner
// succeed, both increments are committed.
func TestRunInTx_NestedBothCommit(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		if _, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 1`); err != nil {
			return err
		}
		return pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			_, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 10`)
			return err
		})
	})
	require.NoError(t, err)

	// Both increments committed: 1 + 10 = 11.
	require.Equal(t, 11, readCounter(t, pool))
}

// TestRunInTx_PanicRollsBackAndRepanics verifies that a panic inside fn
// causes a rollback and the panic propagates to the caller.
func TestRunInTx_PanicRollsBackAndRepanics(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	require.PanicsWithValue(t, "boom", func() {
		_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			if _, err := pg.FromContext(ctx, pool).Exec(ctx, `update counter set n = n + 99`); err != nil {
				return err
			}
			panic("boom")
		})
	})

	// Transaction must have been rolled back — counter is still 0.
	require.Equal(t, 0, readCounter(t, pool))
}

// TestFromContextRead_UsesReaderWithoutTx verifies that FromContextRead
// returns a usable DBTX when no transaction is in context.
func TestFromContextRead_UsesReaderWithoutTx(t *testing.T) {
	pool := setupCounter(t)
	ctx := context.Background()

	dbtx := pg.FromContextRead(ctx, pool)
	require.NotNil(t, dbtx)

	var got int
	require.NoError(t, dbtx.QueryRow(ctx, `select 1`).Scan(&got))
	require.Equal(t, 1, got)
}
