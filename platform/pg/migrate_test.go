package pg_test

import (
	"context"
	"embed"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

//go:embed testdata/migrations/*.sql
var testMigrations embed.FS

func TestMigrate_AppliesEmbeddedMigrations(t *testing.T) {
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	err := pg.Migrate(ctx, dsn, testMigrations, "testdata/migrations")
	require.NoError(t, err)

	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	var exists bool
	err = pool.Writer().QueryRow(ctx,
		`select exists (select 1 from information_schema.tables where table_name='widget')`,
	).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "widget table should exist after migration")
}
