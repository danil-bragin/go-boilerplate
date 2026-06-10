package pg_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPool_ConnectsPingsAndHealthChecks(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn), MaxConns: 5, MinConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	require.NoError(t, pool.HealthCheck(ctx))
	require.Same(t, pool.Writer(), pool.Reader(), "reader falls back to writer when no replica")

	var got int
	err = pool.Writer().QueryRow(ctx, "select 1").Scan(&got)
	require.NoError(t, err)
	require.Equal(t, 1, got)
}

// TestPool_ReaderSizingAndStatementTimeout (integration): reader pool honors
// PG_READER_MAX_CONNS/PG_READER_MIN_CONNS, and the default statement_timeout
// runtime param is live on BOTH pools (asserted via SHOW statement_timeout).
func TestPool_ReaderSizingAndStatementTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)

	cfg := pg.Config{
		DSN:              config.Secret(dsn),
		ReaderDSN:        config.Secret(dsn), // same instance, distinct pool
		MaxConns:         10,
		MinConns:         1,
		ReaderMaxConns:   3,
		ReaderMinConns:   2,
		StatementTimeout: 7 * time.Second,
	}
	pool, err := pg.New(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	require.Equal(t, int32(10), pool.Writer().Config().MaxConns)
	require.Equal(t, int32(3), pool.Reader().Config().MaxConns, "PG_READER_MAX_CONNS must size the reader pool")
	require.Equal(t, int32(2), pool.Reader().Config().MinConns, "PG_READER_MIN_CONNS must size the reader pool")

	for name, q := range map[string]interface {
		QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	}{"writer": pool.Writer(), "reader": pool.Reader()} {
		var timeout string
		require.NoError(t, q.QueryRow(ctx, `SHOW statement_timeout`).Scan(&timeout))
		require.Equal(t, "7s", timeout, "%s pool must run with the configured statement_timeout", name)
	}
}

// TestPool_ReaderSizingDefaultsToWriter (integration): with reader sizing left
// at zero, the reader pool inherits the writer's MaxConns/MinConns.
func TestPool_ReaderSizingDefaultsToWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)

	cfg := pg.Config{DSN: config.Secret(dsn), ReaderDSN: config.Secret(dsn), MaxConns: 8, MinConns: 2}
	pool, err := pg.New(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	require.Equal(t, int32(8), pool.Reader().Config().MaxConns, "reader defaults to writer MaxConns")
	require.Equal(t, int32(2), pool.Reader().Config().MinConns, "reader defaults to writer MinConns")
}

// TestPool_TimestamptzScansUTC pins the ScanLocation registration: without
// it pgx returns timestamptz values in time.Local, making behavior depend
// on the host (or container) timezone. Both pool handles must scan UTC even
// when the SESSION timezone is changed — the wire value is an absolute
// instant; only its Go representation is at stake.
func TestPool_TimestamptzScansUTC(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	ctx := context.Background()
	dsn := pgtest.SharedDSN(t)

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	for name, p := range map[string]*pgxpool.Pool{"writer": pool.Writer(), "reader": pool.Reader()} {
		t.Run(name, func(t *testing.T) {
			conn, err := p.Acquire(ctx)
			require.NoError(t, err)
			defer conn.Release()

			_, err = conn.Exec(ctx, `SET TIME ZONE 'America/New_York'`)
			require.NoError(t, err)

			var ts time.Time
			require.NoError(t, conn.QueryRow(ctx, `select now()`).Scan(&ts))
			require.Equal(t, time.UTC, ts.Location(),
				"timestamptz must scan as UTC regardless of session time zone")
		})
	}
}
