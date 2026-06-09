package pg_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

func TestPool_ConnectsPingsAndHealthChecks(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	pool, err := pg.New(ctx, pg.Config{DSN: dsn, MaxConns: 5, MinConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	require.NoError(t, pool.HealthCheck(ctx))
	require.Same(t, pool.Writer(), pool.Reader(), "reader falls back to writer when no replica")

	var got int
	err = pool.Writer().QueryRow(ctx, "select 1").Scan(&got)
	require.NoError(t, err)
	require.Equal(t, 1, got)
}
