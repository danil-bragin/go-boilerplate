package pg_test

import (
	"context"
	"database/sql"
	"embed"
	"sync"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

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

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
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
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
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

//go:embed testdata/lockcheck/*.sql
var lockcheckMigrations embed.FS

// TestMigrate_LockAndMigrationShareSession: the advisory lock and the goose
// migration statements MUST execute on the same Postgres session. The
// embedded migration raises if pg_locks shows no advisory lock held by
// pg_backend_pid() — i.e. if goose ran on a different connection than the
// lock holder (the historical bug: lock on a dedicated conn, goose on the
// shared *sql.DB pool).
func TestMigrate_LockAndMigrationShareSession(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	require.NoError(t, pg.Migrate(ctx, dsn, lockcheckMigrations, "testdata/lockcheck"),
		"migration must run on the session holding the advisory lock")
}

// TestConfig_MigrateDSN: PG_MIGRATE_URL overrides the pool DSN for migrations
// (required behind PgBouncer transaction pooling, where session advisory
// locks and migrations need a direct Postgres connection).
func TestConfig_MigrateDSN(t *testing.T) {
	cfg := pg.Config{DSN: "postgres://pooled:5432/db"}
	require.Equal(t, "postgres://pooled:5432/db", cfg.MigrateDSN().Reveal(), "default: pool DSN")

	cfg.MigrateURL = "postgres://direct:5432/db"
	require.Equal(t, "postgres://direct:5432/db", cfg.MigrateDSN().Reveal(), "PG_MIGRATE_URL wins when set")
}

// TestMigrate_MigrateURLOverrideHonored (integration): when MigrateURL is set
// it is what Migrate should dial — proven by giving servicekit-style callers
// a valid MigrateDSN while the pool DSN is unreachable garbage.
func TestMigrate_MigrateURLOverrideHonored(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	cfg := pg.Config{
		DSN:        "postgres://nobody:nope@127.0.0.1:1/void", // unreachable
		MigrateURL: config.Secret(dsn),
	}
	require.NoError(t, pg.Migrate(ctx, cfg.MigrateDSN().Reveal(), testMigrations, "testdata/migrations"))
}
