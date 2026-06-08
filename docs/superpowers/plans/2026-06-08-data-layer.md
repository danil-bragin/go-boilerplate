# Data Layer (Sub-project 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the reusable Postgres data layer: a tuned `pgxpool` factory with reader/writer split + health, a transaction runner that carries the tx in `context` (the seam the future CQRS Transaction behavior plugs into), a goose migration runner over embedded SQL, sqlc-generated type-safe queries, and a transactional `outbox` (enqueue within the business tx + a polling relay over a transport-agnostic `Publisher` interface).

**Architecture:** All packages live under `platform/` (no business logic). Repositories never open transactions — they pull a `DBTX` (pool or tx) from `context` via `pg.FromContext`. `pg.RunInTx` is the ONLY place a write tx is opened in this sub-project; it stores the tx in context so any sqlc querier inside the callback participates. The outbox writes to its table using that same context-bound tx, guaranteeing the event and the business data commit atomically. The relay polls unpublished rows and hands them to an injected `Publisher` — Kafka is NOT imported here (SP3 provides the Kafka `Publisher` impl). Integration tests use testcontainers-go with a real Postgres.

**Tech Stack:** `github.com/jackc/pgx/v5` + `pgxpool` · `github.com/pressly/goose/v3` · `sqlc` (generates pgx/v5 code) · `github.com/jackc/pgx/v5/stdlib` (goose `*sql.DB`) · `github.com/testcontainers/testcontainers-go` + `.../modules/postgres` · `github.com/stretchr/testify` · `github.com/google/uuid`.

**Depends on (SP1, done):** `platform/config`, `platform/log`, `platform/run` (Closer), `platform/health` (the pool health check registers here).

**Out of scope (later sub-projects):** Kafka `Publisher` impl + serde (SP3), CQRS Transaction behavior that calls `pg.RunInTx` (SP4), cache/blob/resilience/auth (SP5), example services (SP6), compose/CI (SP7).

**Prerequisite for running tests:** Docker must be available (testcontainers spins up Postgres). `sqlc` CLI must be installed for the generate step (`go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest`); generated code IS committed so consumers don't need sqlc.

---

## File Structure

```
go-boilerplate/
├── platform/
│   ├── pg/
│   │   ├── config.go            Config (DSN, pool sizing, timeouts) + cleanenv tags
│   │   ├── pool.go              New() reader/writer pools, Pool wrapper, health check, Close
│   │   ├── tx.go                DBTX interface, FromContext, RunInTx (tx→ctx), Querier helper
│   │   ├── migrate.go           goose runner over an embedded fs.FS
│   │   ├── pgtest/
│   │   │   └── pgtest.go        testcontainers Postgres helper (NewContainer) for tests
│   │   ├── config_test.go
│   │   ├── pool_test.go         (testcontainers)
│   │   ├── tx_test.go           (testcontainers)
│   │   └── migrate_test.go      (testcontainers)
│   └── outbox/
│       ├── migrations/
│       │   └── 00001_outbox.sql goose migration: outbox table
│       ├── queries/
│       │   └── outbox.sql        sqlc queries (insert, fetch unpublished, mark published)
│       ├── sqlc.yaml             sqlc config for this package
│       ├── gen/                  sqlc OUTPUT (committed): models.go, db.go, outbox.sql.go
│       ├── message.go           Message domain type + Publisher interface
│       ├── repository.go         Repository.Enqueue (uses ctx DBTX + sqlc)
│       ├── relay.go              Relay: poll unpublished → Publisher → mark published
│       ├── repository_test.go    (testcontainers)
│       └── relay_test.go         (testcontainers, fake Publisher)
└── (sqlc must be on PATH for the generate step; generated code committed)
```

**Boundary rule (unchanged):** `platform/*` never imports `cmd/` or `examples/`. `platform/outbox` imports `platform/pg`. Neither imports Kafka.

---

## Task 1: `platform/pg` — config + reader/writer pools + health

**Files:**
- Create: `platform/pg/config.go`, `platform/pg/pool.go`, `platform/pg/pgtest/pgtest.go`
- Test: `platform/pg/config_test.go`, `platform/pg/pool_test.go`

- [ ] **Step 1: Write the config test (no container needed)**

`platform/pg/config_test.go`:
```go
package pg_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
)

func TestConfig_PoolConfigAppliesSizing(t *testing.T) {
	cfg := pg.Config{
		DSN:             "postgres://u:p@localhost:5432/db",
		MaxConns:        20,
		MinConns:        2,
		MaxConnLifetime: 0, // exercise default-fill
	}
	pc, err := cfg.poolConfigForTest()
	require.NoError(t, err)
	require.Equal(t, int32(20), pc.MaxConns)
	require.Equal(t, int32(2), pc.MinConns)
	require.NotZero(t, pc.MaxConnLifetime, "zero lifetime must be defaulted")
}
```

> The test calls an unexported `poolConfigForTest`. Provide it as a thin exported-for-test wrapper in an in-package test file OR rename: simplest is to make the builder method exported as `BuildPoolConfig`. Use `BuildPoolConfig` in the test instead of `poolConfigForTest`. Rewrite the test's call to `cfg.BuildPoolConfig()` and the assertion stays identical.

Final `config_test.go` (use this exact version):
```go
package pg_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
)

func TestConfig_BuildPoolConfigAppliesSizingAndDefaults(t *testing.T) {
	cfg := pg.Config{
		DSN:             "postgres://u:p@localhost:5432/db",
		MaxConns:        20,
		MinConns:        2,
		MaxConnLifetime: 0, // exercise default-fill
	}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, int32(20), pc.MaxConns)
	require.Equal(t, int32(2), pc.MinConns)
	require.NotZero(t, pc.MaxConnLifetime, "zero lifetime must be defaulted")
}
```

Run: `go test ./platform/pg/... -run TestConfig` → FAIL (package missing).

- [ ] **Step 2: Write `platform/pg/config.go`**

```go
// Package pg provides a tuned pgx connection pool with reader/writer split,
// a transaction runner that carries the active transaction in context, and a
// goose-based migration runner. Repositories pull a DBTX from context via
// FromContext; they never open transactions themselves.
package pg

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures a Postgres connection pool.
//
// ReaderDSN is optional; when empty, reads use the same (writer) pool.
type Config struct {
	DSN               string        `env:"PG_DSN" env-default:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
	ReaderDSN         string        `env:"PG_READER_DSN" env-default:""`
	MaxConns          int32         `env:"PG_MAX_CONNS" env-default:"10"`
	MinConns          int32         `env:"PG_MIN_CONNS" env-default:"2"`
	MaxConnLifetime   time.Duration `env:"PG_MAX_CONN_LIFETIME" env-default:"30m"`
	MaxConnIdleTime   time.Duration `env:"PG_MAX_CONN_IDLE_TIME" env-default:"5m"`
	HealthCheckPeriod time.Duration `env:"PG_HEALTH_CHECK_PERIOD" env-default:"1m"`
}

// BuildPoolConfig parses DSN into a *pgxpool.Config and applies sizing/timeout
// settings, defaulting zero durations to sane values.
func (c Config) BuildPoolConfig() (*pgxpool.Config, error) {
	return c.buildPoolConfig(c.DSN)
}

func (c Config) buildPoolConfig(dsn string) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if c.MaxConns > 0 {
		pc.MaxConns = c.MaxConns
	}
	if c.MinConns > 0 {
		pc.MinConns = c.MinConns
	}
	pc.MaxConnLifetime = nonZeroDur(c.MaxConnLifetime, 30*time.Minute)
	pc.MaxConnIdleTime = nonZeroDur(c.MaxConnIdleTime, 5*time.Minute)
	pc.HealthCheckPeriod = nonZeroDur(c.HealthCheckPeriod, time.Minute)
	return pc, nil
}

func nonZeroDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
```

Run config test → PASS.

- [ ] **Step 3: Write `platform/pg/pool.go`**

```go
package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool holds the writer pool and (optionally distinct) reader pool.
// When no ReaderDSN is configured, Reader() returns the writer pool.
type Pool struct {
	writer *pgxpool.Pool
	reader *pgxpool.Pool
}

// New builds writer (and optional reader) pools and verifies connectivity.
func New(ctx context.Context, cfg Config) (*Pool, error) {
	wc, err := cfg.buildPoolConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	writer, err := pgxpool.NewWithConfig(ctx, wc)
	if err != nil {
		return nil, fmt.Errorf("pg: new writer pool: %w", err)
	}
	if err := writer.Ping(ctx); err != nil {
		writer.Close()
		return nil, fmt.Errorf("pg: ping writer: %w", err)
	}

	p := &Pool{writer: writer, reader: writer}

	if cfg.ReaderDSN != "" {
		rc, err := cfg.buildPoolConfig(cfg.ReaderDSN)
		if err != nil {
			writer.Close()
			return nil, err
		}
		reader, err := pgxpool.NewWithConfig(ctx, rc)
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("pg: new reader pool: %w", err)
		}
		if err := reader.Ping(ctx); err != nil {
			writer.Close()
			reader.Close()
			return nil, fmt.Errorf("pg: ping reader: %w", err)
		}
		p.reader = reader
	}
	return p, nil
}

// Writer returns the writer pool (use for writes and read-your-writes).
func (p *Pool) Writer() *pgxpool.Pool { return p.writer }

// Reader returns the reader pool (the writer pool if no replica configured).
func (p *Pool) Reader() *pgxpool.Pool { return p.reader }

// HealthCheck pings the writer (and reader, if distinct). Suitable for
// registering with platform/health.AddReadiness.
func (p *Pool) HealthCheck(ctx context.Context) error {
	if err := p.writer.Ping(ctx); err != nil {
		return fmt.Errorf("pg: writer unhealthy: %w", err)
	}
	if p.reader != p.writer {
		if err := p.reader.Ping(ctx); err != nil {
			return fmt.Errorf("pg: reader unhealthy: %w", err)
		}
	}
	return nil
}

// Close closes both pools. Safe to register with run.Closer.
func (p *Pool) Close(_ context.Context) error {
	if p.reader != p.writer && p.reader != nil {
		p.reader.Close()
	}
	if p.writer != nil {
		p.writer.Close()
	}
	return nil
}
```

- [ ] **Step 4: Write the testcontainers helper `platform/pg/pgtest/pgtest.go`**

```go
// Package pgtest spins up a throwaway Postgres for integration tests.
package pgtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewDSN starts a Postgres container and returns its DSN. The container is
// terminated automatically when the test finishes.
func NewDSN(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
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
```

> If `postgres.Run` is not present in the resolved testcontainers-go modules version, the older API is `postgres.RunContainer(ctx, testcontainers.WithImage("postgres:16-alpine"), ...)`. Run `go doc github.com/testcontainers/testcontainers-go/modules/postgres` and use whichever constructor exists; keep the options identical. Report which you used.

- [ ] **Step 5: Write `platform/pg/pool_test.go`**

```go
package pg_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

func TestPool_ConnectsPingsAndHealthChecks(t *testing.T) {
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
```

- [ ] **Step 6: Add deps, run tests**

Run:
```bash
go get github.com/jackc/pgx/v5@latest
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/postgres@latest
go mod tidy
go test ./platform/pg/...
```
Expected: config test PASS immediately; pool test PASS (requires Docker — pulls postgres:16-alpine on first run).

- [ ] **Step 7: Commit**

```bash
git add platform/pg go.mod go.sum
git commit -m "feat(pg): tuned pgxpool with reader/writer split and health check"
```

---

## Task 2: `platform/pg` — DBTX, FromContext, RunInTx

**Files:**
- Create: `platform/pg/tx.go`
- Test: `platform/pg/tx_test.go`

- [ ] **Step 1: Write `platform/pg/tx_test.go`**

```go
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
```

Run: `go test ./platform/pg/... -run 'TestRunInTx|TestFromContext'` → FAIL (undefined).

- [ ] **Step 2: Write `platform/pg/tx.go`**

```go
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// DBTX is the query surface shared by *pgxpool.Pool and pgx.Tx. sqlc-generated
// queriers accept this interface, so the same query code runs inside or
// outside a transaction depending on what FromContext returns.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconnCommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// pgconnCommandTag aliases pgconn.CommandTag without importing pgconn at call
// sites; both *pgxpool.Pool and pgx.Tx return pgconn.CommandTag from Exec.
type pgconnCommandTag = pgconnTag

type txCtxKey struct{}

// FromContext returns the transaction bound to ctx by RunInTx, or the pool's
// writer when no transaction is active.
func FromContext(ctx context.Context, p *Pool) DBTX {
	if tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return p.Writer()
}

// RunInTx begins a transaction on the writer pool, stores it in the context,
// invokes fn, and commits on success or rolls back on error/panic. This is the
// single place a write transaction is opened.
func RunInTx(ctx context.Context, p *Pool, fn func(ctx context.Context) error) (err error) {
	tx, err := p.Writer().Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: begin tx: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback(ctx)
			panic(r)
		}
	}()

	if err := fn(context.WithValue(ctx, txCtxKey{}, tx)); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("pg: rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: commit: %w", err)
	}
	return nil
}
```

> IMPORTANT TYPE NOTE: `*pgxpool.Pool.Exec` and `pgx.Tx.Exec` both return `pgconn.CommandTag`. The `pgconnCommandTag = pgconnTag` indirection above is a placeholder to avoid a stray import — DELETE it and instead import pgconn directly. Replace the DBTX interface's Exec return type with `pgconn.CommandTag` and add `"github.com/jackc/pgx/v5/pgconn"` to imports. Remove the `pgconnCommandTag`/`pgconnTag` alias lines entirely. Final DBTX:
> ```go
> import (
> 	"context"
> 	"errors"
> 	"fmt"
> 	"github.com/jackc/pgx/v5"
> 	"github.com/jackc/pgx/v5/pgconn"
> )
> type DBTX interface {
> 	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
> 	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
> 	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
> }
> ```
> This DBTX shape is exactly what sqlc generates for `sql_package: pgx/v5`, so generated queriers satisfy it.

- [ ] **Step 3: Run tx tests**

Run: `go test ./platform/pg/...`
Expected: all PASS (config + pool + tx).

- [ ] **Step 4: Commit**

```bash
git add platform/pg/tx.go platform/pg/tx_test.go
git commit -m "feat(pg): context-bound DBTX and RunInTx transaction runner"
```

---

## Task 3: `platform/pg` — goose migration runner over embedded FS

**Files:**
- Create: `platform/pg/migrate.go`
- Test: `platform/pg/migrate_test.go`

- [ ] **Step 1: Write `platform/pg/migrate_test.go`**

```go
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
```

- [ ] **Step 2: Create the test migration fixture**

`platform/pg/testdata/migrations/00001_widget.sql`:
```sql
-- +goose Up
create table widget (
    id   bigserial primary key,
    name text not null
);

-- +goose Down
drop table widget;
```

Run: `go test ./platform/pg/... -run TestMigrate` → FAIL (pg.Migrate undefined).

- [ ] **Step 3: Write `platform/pg/migrate.go`**

```go
package pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Migrate applies all up-migrations from fsys (rooted at dir) to the database
// at dsn using goose. Migrations are plain *.sql files with goose Up/Down
// annotations. It opens a short-lived database/sql connection via the pgx
// stdlib driver because goose operates on *sql.DB.
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("pg: open sql db for migration: %w", err)
	}
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(fsys)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("pg: goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, dir); err != nil {
		return fmt.Errorf("pg: goose up: %w", err)
	}
	return nil
}

// ensure the pgx stdlib driver is registered (imported for side effects).
var _ = stdlib.GetDefaultDriver
```

> Notes for the implementer:
> - `stdlib.GetDefaultDriver` reference forces the `stdlib` import that registers the `"pgx"` `database/sql` driver. If `goimports`/lint prefers a blank import, replace the named import + `var _` line with `_ "github.com/jackc/pgx/v5/stdlib"` and delete the `var _` line. Prefer the blank import form if the linter complains.
> - If `goose.UpContext` does not exist in the resolved goose/v3 version, use `goose.Up(db, dir)` (non-context). Run `go doc github.com/pressly/goose/v3 Up` to confirm and report which you used.
> - `goose.SetBaseFS`/`SetDialect` are global; that is acceptable here (migrations run once at startup, single-threaded).

- [ ] **Step 4: Run migration test**

Run:
```bash
go get github.com/pressly/goose/v3@latest
go mod tidy
go test ./platform/pg/... -run TestMigrate
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/pg/migrate.go platform/pg/migrate_test.go platform/pg/testdata go.mod go.sum
git commit -m "feat(pg): goose migration runner over embedded fs"
```

---

## Task 4: `platform/outbox` — schema migration + sqlc setup + generated code

**Files:**
- Create: `platform/outbox/migrations/00001_outbox.sql`, `platform/outbox/queries/outbox.sql`, `platform/outbox/sqlc.yaml`
- Generate (committed): `platform/outbox/gen/*.go`

- [ ] **Step 1: Write the outbox migration `platform/outbox/migrations/00001_outbox.sql`**

```sql
-- +goose Up
create table outbox (
    id             uuid primary key,
    aggregate_type text        not null,
    aggregate_id   text        not null,
    event_type     text        not null,
    payload        bytea       not null,
    headers        jsonb       not null default '{}'::jsonb,
    created_at     timestamptz not null default now(),
    published_at   timestamptz
);

create index outbox_unpublished_idx on outbox (created_at) where published_at is null;

-- +goose Down
drop table outbox;
```

- [ ] **Step 2: Write the sqlc queries `platform/outbox/queries/outbox.sql`**

```sql
-- name: InsertOutbox :exec
insert into outbox (id, aggregate_type, aggregate_id, event_type, payload, headers)
values ($1, $2, $3, $4, $5, $6);

-- name: FetchUnpublished :many
select id, aggregate_type, aggregate_id, event_type, payload, headers, created_at
from outbox
where published_at is null
order by created_at
limit $1
for update skip locked;

-- name: MarkPublished :exec
update outbox set published_at = now() where id = $1;
```

- [ ] **Step 3: Write `platform/outbox/sqlc.yaml`**

```yaml
version: "2"
sql:
  - engine: "postgresql"
    queries: "queries"
    schema: "migrations"
    gen:
      go:
        package: "gen"
        out: "gen"
        sql_package: "pgx/v5"
        emit_json_tags: false
        emit_pointers_for_null_types: true
        overrides:
          - db_type: "uuid"
            go_type: "github.com/google/uuid.UUID"
          - db_type: "jsonb"
            go_type: "[]byte"
```

> `schema: "migrations"` lets sqlc read the goose migration as the schema source. goose's `-- +goose Up/Down` comment lines are ignored by sqlc's parser (they are SQL comments). If sqlc errors on the goose annotations in the resolved version, create `platform/outbox/migrations/schema.sql` containing ONLY the `create table`/`create index` statements (no goose comments) and point `schema:` at that file instead; keep the goose migration for runtime. Report which approach was used.

- [ ] **Step 4: Generate the code**

Run:
```bash
cd platform/outbox
sqlc generate
cd ../..
go get github.com/google/uuid@latest
go mod tidy
go build ./platform/outbox/...
```
Expected: `platform/outbox/gen/` now contains `db.go`, `models.go`, `outbox.sql.go`; the package builds. The generated `Querier`/`Queries` accept a `DBTX` interface matching `pg.DBTX` (Exec/Query/QueryRow with pgconn.CommandTag).

> If `sqlc` is not installed: `go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest` and ensure `$(go env GOPATH)/bin` is on PATH. The generated code is committed, so this is a one-time author step.

- [ ] **Step 5: Commit**

```bash
git add platform/outbox/migrations platform/outbox/queries platform/outbox/sqlc.yaml platform/outbox/gen go.mod go.sum
git commit -m "feat(outbox): schema migration and sqlc-generated queries"
```

---

## Task 5: `platform/outbox` — Message type, Publisher interface, Repository.Enqueue

**Files:**
- Create: `platform/outbox/message.go`, `platform/outbox/repository.go`
- Test: `platform/outbox/repository_test.go`

- [ ] **Step 1: Write `platform/outbox/message.go`**

```go
// Package outbox implements the transactional outbox pattern: events are
// written to an outbox table within the same transaction as the business
// data (Repository.Enqueue), and a Relay later publishes them to a transport
// via the Publisher interface. The transport (e.g. Kafka) is injected; this
// package does not depend on any broker.
package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Message is one outbox record to be published.
type Message struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Headers       []byte // JSON object; defaults to {} when nil
	CreatedAt     time.Time
}

// Publisher delivers an outbox message to a transport. Implemented by the
// Kafka adapter in a later sub-project. It must be safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}
```

- [ ] **Step 2: Write `platform/outbox/repository_test.go`**

```go
package outbox_test

import (
	"context"
	"embed"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

//go:embed migrations/*.sql
var migrations embed.FS

func newPoolWithSchema(t *testing.T) *pg.Pool {
	t.Helper()
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

func TestEnqueue_WritesRowWithinTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID:            id,
			AggregateType: "order",
			AggregateID:   "42",
			EventType:     "OrderCreated",
			Payload:       []byte(`{"id":"42"}`),
		})
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id=$1 and published_at is null`, id,
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEnqueue_RolledBackWithFailedTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_ = repo.Enqueue(ctx, outbox.Message{
			ID: id, AggregateType: "order", AggregateID: "1",
			EventType: "X", Payload: []byte(`{}`),
		})
		return context.Canceled // force rollback
	})

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id=$1`, id).Scan(&count))
	require.Equal(t, 0, count, "enqueue must roll back with the business tx")
}
```

Run: `go test ./platform/outbox/... -run TestEnqueue` → FAIL (NewRepository undefined).

- [ ] **Step 3: Write `platform/outbox/repository.go`**

```go
package outbox

import (
	"context"
	"fmt"

	"go-boilerplate/platform/outbox/gen"
	"go-boilerplate/platform/pg"
)

// Repository persists outbox messages using the transaction bound to the
// context (via pg.RunInTx), so an enqueue commits atomically with the
// business data written in the same transaction.
type Repository struct {
	pool *pg.Pool
}

// NewRepository creates a Repository over the given pool.
func NewRepository(pool *pg.Pool) *Repository {
	return &Repository{pool: pool}
}

// Enqueue inserts a message into the outbox using the context's DBTX.
func (r *Repository) Enqueue(ctx context.Context, msg Message) error {
	headers := msg.Headers
	if headers == nil {
		headers = []byte("{}")
	}
	q := gen.New(pg.FromContext(ctx, r.pool))
	err := q.InsertOutbox(ctx, gen.InsertOutboxParams{
		ID:            msg.ID,
		AggregateType: msg.AggregateType,
		AggregateID:   msg.AggregateID,
		EventType:     msg.EventType,
		Payload:       msg.Payload,
		Headers:       headers,
	})
	if err != nil {
		return fmt.Errorf("outbox: enqueue: %w", err)
	}
	return nil
}
```

> The exact generated names (`gen.New`, `gen.InsertOutboxParams`, field names) come from Task 4's sqlc output. Open `platform/outbox/gen/outbox.sql.go` to confirm the param struct field names (sqlc derives them from column names: `ID`, `AggregateType`, etc.). If `gen.New` expects a `gen.DBTX` rather than `pg.DBTX`, they are structurally identical interfaces — but Go requires the exact named interface. Pass `pg.FromContext(...)` directly; since `gen.DBTX` and `pg.DBTX` have identical method sets, a `pg.DBTX` value satisfies `gen.DBTX` ONLY if `gen.New` takes an interface (it does). If a compile error arises about interface mismatch, adjust by having `gen.New` receive the value directly (interfaces are structural at the call boundary for assignment to a named interface param — this works). Report any adjustment.

- [ ] **Step 4: Run enqueue tests**

Run: `go test ./platform/outbox/... -run TestEnqueue`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/outbox/message.go platform/outbox/repository.go platform/outbox/repository_test.go
git commit -m "feat(outbox): message type, Publisher interface, transactional Enqueue"
```

---

## Task 6: `platform/outbox` — Relay (poll → publish → mark published)

**Files:**
- Create: `platform/outbox/relay.go`
- Test: `platform/outbox/relay_test.go`

- [ ] **Step 1: Write `platform/outbox/relay_test.go`**

```go
package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/pg"
)

type fakePublisher struct {
	mu       sync.Mutex
	received []outbox.Message
	failNext bool
}

func (f *fakePublisher) Publish(_ context.Context, msg outbox.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return context.DeadlineExceeded
	}
	f.received = append(f.received, msg)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func TestRelay_PublishesUnpublishedAndMarksThem(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	for i := 0; i < 3; i++ {
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: uuid.New(), AggregateType: "order", AggregateID: "x",
				EventType: "OrderCreated", Payload: []byte(`{}`),
			})
		}))
	}

	pub := &fakePublisher{}
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	n, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, 3, pub.count())

	// All marked published → a second batch processes nothing.
	n2, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n2)
}

func TestRelay_PublishFailureLeavesRowUnpublished(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID: uuid.New(), AggregateType: "order", AggregateID: "x",
			EventType: "OrderCreated", Payload: []byte(`{}`),
		})
	}))

	pub := &fakePublisher{failNext: true}
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	_, err := relay.ProcessBatch(ctx)
	require.Error(t, err)

	var unpublished int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&unpublished))
	require.Equal(t, 1, unpublished, "failed publish must not mark row published")
}
```

Run: `go test ./platform/outbox/... -run TestRelay` → FAIL (NewRelay undefined).

- [ ] **Step 2: Write `platform/outbox/relay.go`**

```go
package outbox

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/outbox/gen"
	"go-boilerplate/platform/pg"
)

// RelayConfig configures the polling relay.
type RelayConfig struct {
	BatchSize    int32         `env:"OUTBOX_BATCH_SIZE" env-default:"100"`
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" env-default:"1s"`
}

// Relay polls unpublished outbox rows and publishes them via a Publisher,
// marking each published only after a successful Publish. Each batch runs in a
// transaction using `for update skip locked`, so multiple relay instances can
// run concurrently without double-publishing.
type Relay struct {
	pool *pg.Pool
	pub  Publisher
	cfg  RelayConfig
}

// NewRelay creates a Relay.
func NewRelay(pool *pg.Pool, pub Publisher, cfg RelayConfig) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Relay{pool: pool, pub: pub, cfg: cfg}
}

// ProcessBatch fetches up to BatchSize unpublished messages (locking them),
// publishes each, and marks them published — all in one transaction. It
// returns the number of messages successfully published. A publish error
// aborts the batch (transaction rolls back), leaving rows unpublished for
// retry on the next poll.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	var published int
	err := pg.RunInTx(ctx, r.pool, func(ctx context.Context) error {
		q := gen.New(pg.FromContext(ctx, r.pool))
		rows, err := q.FetchUnpublished(ctx, r.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("outbox: fetch: %w", err)
		}
		for _, row := range rows {
			msg := Message{
				ID:            row.ID,
				AggregateType: row.AggregateType,
				AggregateID:   row.AggregateID,
				EventType:     row.EventType,
				Payload:       row.Payload,
				Headers:       row.Headers,
				CreatedAt:     row.CreatedAt,
			}
			if err := r.pub.Publish(ctx, msg); err != nil {
				return fmt.Errorf("outbox: publish %s: %w", msg.ID, err)
			}
			if err := q.MarkPublished(ctx, msg.ID); err != nil {
				return fmt.Errorf("outbox: mark published %s: %w", msg.ID, err)
			}
			published++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, nil
}

// Run polls until ctx is canceled, processing a batch every PollInterval.
// Intended to be launched in a goroutine; returns ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.ProcessBatch(ctx); err != nil {
				// Transient: log and keep polling. Caller wires a logger;
				// here we swallow to avoid killing the loop on one failure.
				continue
			}
		}
	}
}
```

> Field-name note: `row.CreatedAt`, `row.Payload`, `row.Headers`, etc. come from the sqlc-generated `FetchUnpublishedRow` struct. Open `platform/outbox/gen/outbox.sql.go` and match the exact field names (sqlc may name the row struct `FetchUnpublishedRow`). If `MarkPublished` takes a different param type, adjust. `Headers` maps to `[]byte` per the sqlc override; `CreatedAt` is `time.Time` (or `pgtype.Timestamptz` if the override wasn't applied — if so, use `.Time`). Report any field/type adjustments.

> The `Run` loop swallows batch errors to stay alive (a dead relay loses all events). SP5 wires a real logger + metrics here; for now the silent `continue` is acceptable and noted. Do NOT add a logger dependency in this sub-project.

- [ ] **Step 3: Run relay tests**

Run: `go test ./platform/outbox/... -run TestRelay`
Expected: PASS (both tests).

- [ ] **Step 4: Full suite + lint**

Run:
```bash
go test -race -count=1 ./...
go vet ./...
gofmt -l .
golangci-lint run ./...
```
Expected: all PASS / clean. (Data-layer tests require Docker for testcontainers.)

- [ ] **Step 5: Commit**

```bash
git add platform/outbox/relay.go platform/outbox/relay_test.go
git commit -m "feat(outbox): polling relay publishing via transport-agnostic Publisher"
```

---

## Self-Review (completed)

**Spec coverage (against plan.md §2/§3 data-layer rows):**
- pgx + pgxpool tuned (max_conns, lifetime/idle, MinConns>0) ✅ Task 1
- reader/writer split ✅ Task 1 (Pool.Reader/Writer, falls back when no replica)
- health check for platform/health ✅ Task 1 (Pool.HealthCheck)
- tx runner + tx-in-context (the seam for SP4 Transaction behavior) ✅ Task 2 (RunInTx, FromContext, DBTX)
- goose migrations over embedded FS ✅ Task 3
- sqlc (pgx/v5, `:batch`/`:copyfrom` available) ✅ Task 4 (outbox queries; copyfrom/batch demonstrated in SP6 when bulk paths appear)
- transactional outbox: enqueue within business tx ✅ Task 5; polling relay over Publisher interface (Kafka-agnostic) ✅ Task 6 (`for update skip locked` for concurrent relays)
- PgBouncer prepared-stmt note: documented as a deployment concern in plan.md §6; not code in SP2 (pgx talks directly to Postgres in tests). Tracked for SP7 compose.

**Type consistency:** `pg.Config`, `pg.New`, `pg.Pool.{Writer,Reader,HealthCheck,Close}` used identically across Tasks 1–6. `pg.DBTX`, `pg.FromContext`, `pg.RunInTx` consistent Tasks 2,5,6. `pg.Migrate(ctx, dsn, fs.FS, dir)` consistent Tasks 3,5. `outbox.Message`, `outbox.Publisher`, `outbox.NewRepository`/`Enqueue`, `outbox.NewRelay`/`ProcessBatch`/`Run`, `outbox.RelayConfig` consistent Tasks 5–6. The sqlc `gen` package is the one place exact names must be reconciled against generated output — every task that touches `gen.*` has an explicit "open the generated file and match names" note.

**Placeholder scan:** No TBD/TODO. Every code step is complete. The `> Notes` blocks give concrete fallbacks (testcontainers constructor name, goose Up vs UpContext, sqlc schema-from-goose vs schema.sql, stdlib blank import, generated field names) rather than leaving gaps — these are real version-variance points, each with an explicit decision rule.

**Known follow-ups for SP3+:** Kafka `Publisher` impl + serde wires into `outbox.Relay` (SP3). CQRS `Transaction` behavior calls `pg.RunInTx` (SP4). Relay logger/metrics + DLQ-on-permanent-failure (SP5). `cmd/skeleton` (or example services) register `Pool.HealthCheck` with `health.AddReadiness` and `Pool.Close`/`Relay.Run` with the Closer (SP6). PgBouncer transaction-mode + `QueryExecModeDescribeExec` config surfaced in compose (SP7).
```
```
