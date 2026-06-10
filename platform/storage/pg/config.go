// Package pg provides a tuned pgx connection pool with reader/writer split,
// a transaction runner that carries the active transaction in context, and a
// goose-based migration runner. Repositories pull a DBTX from context via
// FromContext; they never open transactions themselves.
package pg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go-boilerplate/platform/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StatementCacheMode controls how pgx executes queries.
//
// pgx v5 extended query protocol behaviour
//
// By default pgx uses QueryExecModeCachedPlan: the driver prepares statements
// on first use and caches the resulting server-side plan. This is the fastest
// option for direct Postgres connections, but it is INCOMPATIBLE with PgBouncer
// in transaction-mode pooling because PgBouncer discards prepared statements
// between transactions, causing "unknown prepared statement" errors.
//
// When running behind PgBouncer in transaction mode, set StatementCacheMode to
// StatementCacheModeDescribeExec. This switches pgx to QueryExecModeDescribeExec:
// pgx describes parameter types once per unique query text (a single round-trip)
// and then executes without a named prepared statement, which is safe across
// PgBouncer sessions.
//
// Summary:
//
//	StatementCacheModeDefault     — pgx default (CachedPlan), fastest, direct Postgres only.
//	StatementCacheModeDescribeExec — safe behind PgBouncer transaction-mode pooling.
type StatementCacheMode int

const (
	// StatementCacheModeDefault leaves pgx's DefaultQueryExecMode unchanged
	// (QueryExecModeCachedPlan). Use for direct Postgres connections.
	StatementCacheModeDefault StatementCacheMode = iota

	// StatementCacheModeDescribeExec sets QueryExecModeDescribeExec.
	// Required when pgx connects through PgBouncer in transaction-mode pooling
	// where server-side prepared statements cannot survive across transactions.
	StatementCacheModeDescribeExec
)

// Config configures a Postgres connection pool.
//
// ReaderDSN is optional; when empty, reads use the same (writer) pool.
//
// # Pool sizing guidance
//
// The default MaxConns (25) is intentionally higher than the old default of 10
// to accommodate a service that shares a single pool across:
//   - HTTP request handlers
//   - A Kafka consumer
//   - An outbox relay
//
// Tune MaxConns per workload. A safe upper bound is:
//
//	postgres max_connections / replica_count  (leave headroom for ops/monitoring)
//
// Example: max_connections=100, 3 replicas → MaxConns ≤ 30 per replica.
//
// Ideally the outbox relay and Kafka consumer should use a separate pool from
// the HTTP request-serving pool so that background tasks cannot exhaust the
// connections available to live traffic. This is noted as guidance; the
// platform does not enforce it.
//
// # Statement cache / PgBouncer compatibility
//
// Set StatementCacheMode to StatementCacheModeDescribeExec when connecting
// through PgBouncer in transaction-mode pooling. See StatementCacheMode docs.
type Config struct {
	// DSN / ReaderDSN / MigrateURL embed the database password, so they are
	// config.Secret: %v/%+v/%#v dumps and slog output print [REDACTED]. The
	// raw value is read via Reveal() only at the pgxpool/migration call sites.
	DSN       config.Secret `env:"PG_DSN" envDefault:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
	ReaderDSN config.Secret `env:"PG_READER_DSN" envDefault:""`

	// MigrateURL, when set, is the DSN Migrate dials instead of DSN. REQUIRED
	// when DSN points at PgBouncer in transaction pooling mode: migrations
	// need a direct-Postgres session for the advisory lock (see Migrate docs).
	MigrateURL        config.Secret `env:"PG_MIGRATE_URL" envDefault:""`
	MaxConns          int32         `env:"PG_MAX_CONNS" envDefault:"25"`
	MinConns          int32         `env:"PG_MIN_CONNS" envDefault:"5"`
	MaxConnLifetime   time.Duration `env:"PG_MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime   time.Duration `env:"PG_MAX_CONN_IDLE_TIME" envDefault:"5m"`
	HealthCheckPeriod time.Duration `env:"PG_HEALTH_CHECK_PERIOD" envDefault:"1m"`

	// ReaderMaxConns / ReaderMinConns size the READER pool independently from
	// the writer (replicas often tolerate more connections than the primary,
	// or the reverse when many service replicas share one read endpoint).
	// Zero (the default) inherits the writer's MaxConns/MinConns.
	ReaderMaxConns int32 `env:"PG_READER_MAX_CONNS" envDefault:"0"`
	ReaderMinConns int32 `env:"PG_READER_MIN_CONNS" envDefault:"0"`

	// ConnectTimeout bounds the TCP+startup handshake of every new connection
	// (pgconn ConnectTimeout). Zero falls back to 5s — never unbounded.
	ConnectTimeout time.Duration `env:"PG_CONNECT_TIMEOUT" envDefault:"5s"`

	// StatementTimeout is applied as the statement_timeout runtime parameter
	// on every connection of BOTH pools: any single statement running longer
	// is cancelled server-side. The env default is 30s; set 0 to disable
	// (e.g. for bulk/analytical workloads). Long-running migrations are NOT
	// affected — Migrate connects separately, not through these pools.
	StatementTimeout time.Duration `env:"PG_STATEMENT_TIMEOUT" envDefault:"30s"`

	// StatementCacheMode controls the pgx query execution mode.
	// Default (0) leaves pgx's CachedPlan behaviour unchanged.
	// Set to StatementCacheModeDescribeExec when behind PgBouncer txn pooling.
	StatementCacheMode StatementCacheMode `env:"PG_STATEMENT_CACHE_MODE" envDefault:"0"`

	// QueryMetrics installs a pgx tracer on both pools that records the
	// pg.query.duration histogram (seconds, {query, pool}) for every query,
	// batch, and CopyFrom — see querytracer.go. Env default is ON; note that
	// a zero-value Config constructed in code (tests) leaves it off.
	QueryMetrics bool `env:"PG_QUERY_METRICS" envDefault:"true"`
}

// BuildPoolConfig parses DSN into a *pgxpool.Config and applies sizing/timeout
// settings, defaulting zero durations to sane values.
//
// Pool-acquire timeouts: pgxpool has no separate acquire-timeout knob —
// Acquire (and every query through the pool) honors the caller's ctx, so a
// request-scoped deadline (cqrs.Deadline, http request ctx) IS the acquire
// bound. Never call pool methods with context.Background() on a request path.
func (c Config) BuildPoolConfig() (*pgxpool.Config, error) {
	return c.buildPoolConfig(c.DSN.Reveal())
}

func (c Config) buildPoolConfig(dsn string) (*pgxpool.Config, error) {
	return c.buildSizedPoolConfig(dsn, c.MaxConns, c.MinConns, "writer")
}

// buildReaderPoolConfig sizes the reader pool from ReaderMaxConns/ReaderMinConns,
// inheriting the writer values when they are zero.
func (c Config) buildReaderPoolConfig(dsn string) (*pgxpool.Config, error) {
	maxConns := c.ReaderMaxConns
	if maxConns <= 0 {
		maxConns = c.MaxConns
	}
	minConns := c.ReaderMinConns
	if minConns <= 0 {
		minConns = c.MinConns
	}
	return c.buildSizedPoolConfig(dsn, maxConns, minConns, "reader")
}

func (c Config) buildSizedPoolConfig(dsn string, maxConns, minConns int32, role string) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if maxConns > 0 {
		pc.MaxConns = maxConns
	}
	if minConns > 0 {
		pc.MinConns = minConns
	}
	pc.MaxConnLifetime = nonZeroDur(c.MaxConnLifetime, 30*time.Minute)
	pc.MaxConnIdleTime = nonZeroDur(c.MaxConnIdleTime, 5*time.Minute)
	pc.HealthCheckPeriod = nonZeroDur(c.HealthCheckPeriod, time.Minute)
	pc.ConnConfig.ConnectTimeout = nonZeroDur(c.ConnectTimeout, 5*time.Second)

	if c.StatementTimeout > 0 {
		if pc.ConnConfig.RuntimeParams == nil {
			pc.ConnConfig.RuntimeParams = map[string]string{}
		}
		pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(c.StatementTimeout.Milliseconds(), 10)
	}

	if c.StatementCacheMode == StatementCacheModeDescribeExec {
		pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	}

	// Query duration metrics: one tracer instance per pool, carrying the
	// pool's ROLE as the bounded "pool" label (writer|reader).
	if c.QueryMetrics {
		pc.ConnConfig.Tracer = newQueryTracer(role)
	}

	// UTC scanning: pgx's default TimestamptzCodec has no ScanLocation, so
	// scanned timestamptz values carry time.Local — behavior silently
	// depends on the host/container timezone. Registering the codec with
	// ScanLocation=UTC on every connection (this builder serves BOTH the
	// writer and reader pools) makes every scanned instant UTC, matching the
	// repository-wide convention (platform/clock).
	//
	// Precision note: Postgres stores timestamptz with MICROsecond
	// precision; Go time.Time carries nanoseconds. Values round-trip through
	// the database truncated to µs — never compare stored timestamps for
	// equality against in-memory ones without truncating to time.Microsecond.
	pc.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name:  "timestamptz",
			OID:   pgtype.TimestamptzOID,
			Codec: &pgtype.TimestamptzCodec{ScanLocation: time.UTC},
		})
		return nil
	}

	return pc, nil
}

// MigrateDSN returns the DSN migrations should dial: MigrateURL when set
// (direct Postgres behind PgBouncer), otherwise the pool DSN. The value stays
// a config.Secret; call Reveal() at the Migrate call site.
func (c Config) MigrateDSN() config.Secret {
	if c.MigrateURL != "" {
		return c.MigrateURL
	}
	return c.DSN
}

func nonZeroDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
