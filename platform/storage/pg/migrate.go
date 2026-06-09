package pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver needed by goose
	"github.com/pressly/goose/v3"
)

// migrateMu serializes concurrent Migrate calls within a single process
// because goose uses process-global state (SetBaseFS, SetDialect).
var migrateMu sync.Mutex

// migrationLockKey is the Postgres advisory-lock key used to serialize
// Migrate across multiple replicas/processes. The value is a documented
// constant derived from the ASCII bytes "gboile" (go-boilerplate migrations).
//
// Only one replica may hold the lock at a time; all others block until the
// lock is released. Because goose.UpContext is idempotent (it checks the
// goose_db_version table before applying), replicas that acquire the lock
// after the first one find all migrations already applied and return quickly.
const migrationLockKey int64 = 0x67626f696c65 // "gboile" in hex

// Migrate applies all up-migrations from fsys (rooted at dir) to the database
// at dsn using goose. Migrations are plain *.sql files with goose Up/Down
// annotations. It opens a short-lived database/sql connection via the pgx
// stdlib driver because goose operates on *sql.DB.
//
// # Concurrency safety
//
// Migrate is safe to call concurrently from multiple goroutines within the
// same process (serialized by migrateMu) AND from multiple replicas at startup
// (serialized by a Postgres session-level advisory lock).
//
// Advisory lock strategy:
//  1. Open a dedicated *sql.Conn and acquire pg_advisory_lock(key) on it.
//     This blocks until no other session holds the same key.
//  2. Run goose.UpContext against the shared *sql.DB pool; goose manages its
//     own connection(s) for the migration transactions.
//  3. Release the advisory lock with pg_advisory_unlock(key) on the same
//     dedicated connection, then close it.
//
// Replica 2's pg_advisory_lock call blocks at step 1 while replica 1 migrates.
// When replica 1 unlocks, replica 2 acquires the lock and runs goose.UpContext,
// which is a no-op because all migrations are already applied.
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("pg: open sql db for migration: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Acquire a dedicated connection for the advisory lock. Advisory locks in
	// Postgres are session-scoped: the lock is released when the session ends,
	// so we must hold this connection open for the duration of the migration.
	lockConn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pg: open advisory lock connection: %w", err)
	}
	defer func() { _ = lockConn.Close() }()

	// Block until we are the only replica running migrations.
	if _, err := lockConn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("pg: acquire advisory lock: %w", err)
	}
	defer func() {
		// Best-effort release; the lock is also auto-released when the
		// connection is closed, but explicit unlock is more immediate.
		_, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	goose.SetBaseFS(fsys)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("pg: goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, dir); err != nil {
		return fmt.Errorf("pg: goose up: %w", err)
	}
	return nil
}
