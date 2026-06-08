package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

func setupCounter(t *testing.T) *pg.Pool {
	t.Helper()
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
