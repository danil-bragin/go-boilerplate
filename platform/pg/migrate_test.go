package pg_test

import (
	"context"
	"database/sql"
	"embed"
	"sync"
	"testing"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"

	"github.com/stretchr/testify/require"
)

//go:embed testdata/migrations/*.sql
var testMigrations embed.FS

func TestMigrate_AppliesEmbeddedMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	err := pg.Migrate(ctx, dsn, testMigrations, "testdata/migrations")
	require.NoError(t, err)

	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	var exists bool
	err = pool.Writer().QueryRow(
		ctx,
		`select exists (select 1 from information_schema.tables where table_name='widget')`,
	).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "widget table should exist after migration")
}

// TestMigrate_ConcurrentReplicasNoError simulates N replicas all calling
// Migrate at startup simultaneously against the same fresh database. The
// advisory lock inside Migrate must serialize them so:
//   - all N calls return nil error, and
//   - the target table exists exactly once (no duplicate/corrupted schema),
//   - goose's version table shows the expected migration version.
func TestMigrate_ConcurrentReplicasNoError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	const replicas = 5
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errors []error
	)

	// Start barrier: all goroutines wait here before calling Migrate so they
	// hit it as simultaneously as possible.
	start := make(chan struct{})

	for range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := pg.Migrate(ctx, dsn, testMigrations, "testdata/migrations")
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		}()
	}

	close(start) // release all goroutines simultaneously
	wg.Wait()

	// All replicas must succeed.
	require.Empty(t, errors, "all concurrent Migrate calls must return nil; got: %v", errors)

	// The widget table must exist exactly once (migration applied correctly).
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	var exists bool
	err = pool.Writer().QueryRow(
		ctx,
		`select exists (select 1 from information_schema.tables where table_name='widget')`,
	).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "widget table must exist after concurrent migration")

	// Goose version table must show exactly one row for the migration (not
	// duplicated), at the expected version number.
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRowContext(
		ctx,
		`select count(*) from goose_db_version where version_id = 1 and is_applied = true`,
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"goose_db_version must have exactly one applied row for version 1; got %d", count)
}
