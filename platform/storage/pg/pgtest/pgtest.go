// Package pgtest spins up a throwaway Postgres for integration tests.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewDSN starts a Postgres container and returns its DSN. The container is
// terminated automatically when the test (or benchmark) finishes. It accepts
// testing.TB so both *testing.T and *testing.B callers can use it.
func NewDSN(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(
		ctx, "postgres:16-alpine",
		postgres.WithDatabase("app"),
		postgres.WithUsername("app"),
		postgres.WithPassword("app"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return dsn
}

// Shared state: one lazily-started Postgres container per test binary
// (i.e. per package). Each SharedDSN call gets its own database inside it,
// so tests stay isolated without paying a container startup each.
var (
	sharedOnce      sync.Once
	sharedContainer *postgres.PostgresContainer
	sharedAdminDSN  string // DSN of the default "app" database (used to CREATE DATABASE)
	errShared       error
)

// SharedDSN returns a DSN pointing at a FRESH database created inside a
// package-shared Postgres container (started on first use). Isolation is per
// database, so tests can run migrations and assert row counts as if they had
// their own server.
//
// The container is NOT terminated via t.Cleanup; pair SharedDSN with a
// TestMain that calls TerminateShared after m.Run:
//
//	func TestMain(m *testing.M) {
//		code := m.Run()
//		pgtest.TerminateShared()
//		os.Exit(code)
//	}
func SharedDSN(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	sharedOnce.Do(func() {
		container, err := postgres.Run(
			ctx, "postgres:16-alpine",
			postgres.WithDatabase("app"),
			postgres.WithUsername("app"),
			postgres.WithPassword("app"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			errShared = fmt.Errorf("start postgres: %w", err)
			return
		}
		sharedContainer = container

		if sharedAdminDSN, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
			errShared = fmt.Errorf("connection string: %w", err)
		}
	})
	if errShared != nil {
		t.Fatalf("pgtest: shared postgres: %v", errShared)
	}

	// Create a unique database for this test ("app_" prefix keeps the
	// identifier valid; the uuid prefix chars are hex, so no quoting needed).
	dbName := "app_" + uuid.NewString()[:8]
	conn, err := pgx.Connect(ctx, sharedAdminDSN)
	require.NoError(t, err, "pgtest: connect to shared postgres")
	_, err = conn.Exec(ctx, "CREATE DATABASE "+dbName)
	_ = conn.Close(ctx)
	require.NoError(t, err, "pgtest: create database %s", dbName)

	u, err := url.Parse(sharedAdminDSN)
	require.NoError(t, err)
	u.Path = "/" + dbName
	return u.String()
}

// TerminateShared stops the shared Postgres container if one was started.
// Call it from TestMain after m.Run() (safe to call when no test used
// SharedDSN; the testcontainers reaper is the fallback if a TestMain is
// missing).
func TerminateShared() {
	if sharedContainer != nil {
		_ = sharedContainer.Terminate(context.Background())
		sharedContainer = nil
	}
}
