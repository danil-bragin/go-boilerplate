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
// annotations.
//
// # Connection requirements
//
// dsn MUST point directly at Postgres, NOT through PgBouncer in transaction
// pooling mode: Migrate relies on a session-level advisory lock, and pooled
// transaction mode breaks session identity. When the application pool runs
// behind PgBouncer, set PG_MIGRATE_URL to a direct-Postgres DSN and pass
// Config.MigrateDSN() here (servicekit does this automatically).
//
// # Concurrency safety
//
// Migrate is safe to call concurrently from multiple goroutines within the
// same process (serialized by migrateMu) AND from multiple replicas at startup
// (serialized by the Postgres session-level advisory lock).
//
// Single-session strategy: the *sql.DB is capped at ONE connection
// (SetMaxOpenConns(1)), so the advisory lock and every goose statement run on
// the SAME Postgres session:
//  1. Acquire the only connection, take pg_advisory_lock(key) on it, then
//     return it to the (single-slot) pool — the session stays open and keeps
//     holding the lock.
//  2. goose.UpContext draws from the same single-slot pool, i.e. the locked
//     session, and applies migrations on it.
//  3. pg_advisory_unlock(key) executes on the same session; closing the DB
//     would also release the lock (session end), the explicit unlock is just
//     more immediate.
//
// Replica 2's pg_advisory_lock call blocks at step 1 while replica 1 migrates.
// When replica 1 unlocks, replica 2 acquires the lock and runs goose.UpContext,
// which is a no-op because all migrations are already applied.
//
// # Long-running / concurrent index builds
//
// goose wraps each migration in a transaction by default. CREATE INDEX
// CONCURRENTLY cannot run inside a transaction — annotate such migrations
// with `-- +goose NO TRANSACTION` at the top of the file:
//
//	-- +goose NO TRANSACTION
//	-- +goose Up
//	CREATE INDEX CONCURRENTLY orders_status_idx ON orders (status);
//
// Statements in a NO TRANSACTION migration auto-commit individually, so keep
// such files to a single idempotent statement (use IF NOT EXISTS) — a failure
// midway leaves earlier statements applied. The pool's PG_STATEMENT_TIMEOUT
// does not apply here (Migrate dials its own connection), so concurrent index
// builds are not killed by the 30s default.
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("pg: open sql db for migration: %w", err)
	}
	defer func() { _ = db.Close() }()

	// ONE connection total: the advisory lock and the goose statements must
	// share a Postgres session (advisory locks are session-scoped).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // never recycle the locked session mid-migration
	db.SetConnMaxIdleTime(0)

	// Take the lock on the (only) connection, then release the *handle* back
	// to the single-slot pool — the session stays open and keeps the lock, and
	// goose below reuses that exact session. Holding the handle instead would
	// deadlock: goose would wait forever for the one connection.
	lockConn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pg: open advisory lock connection: %w", err)
	}
	if _, err := lockConn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		_ = lockConn.Close()
		return fmt.Errorf("pg: acquire advisory lock: %w", err)
	}
	if err := lockConn.Close(); err != nil { // returns the session to the pool, lock held
		return fmt.Errorf("pg: release lock connection to pool: %w", err)
	}
	defer func() {
		// Best-effort release on the same (single) session; the lock is also
		// auto-released when db.Close() ends the session.
		_, _ = db.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
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
