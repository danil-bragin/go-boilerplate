// Package pg provides a tuned pgx connection pool with reader/writer split,
// a transaction runner that carries the active transaction in context, and a
// goose-based migration runner. Repositories pull a DBTX from context via
// FromContext; they never open transactions themselves.
package pg

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
	DSN               string        `env:"PG_DSN" envDefault:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
	ReaderDSN         string        `env:"PG_READER_DSN" envDefault:""`
	MaxConns          int32         `env:"PG_MAX_CONNS" envDefault:"25"`
	MinConns          int32         `env:"PG_MIN_CONNS" envDefault:"5"`
	MaxConnLifetime   time.Duration `env:"PG_MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime   time.Duration `env:"PG_MAX_CONN_IDLE_TIME" envDefault:"5m"`
	HealthCheckPeriod time.Duration `env:"PG_HEALTH_CHECK_PERIOD" envDefault:"1m"`

	// StatementCacheMode controls the pgx query execution mode.
	// Default (0) leaves pgx's CachedPlan behaviour unchanged.
	// Set to StatementCacheModeDescribeExec when behind PgBouncer txn pooling.
	StatementCacheMode StatementCacheMode `env:"PG_STATEMENT_CACHE_MODE" envDefault:"0"`
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

	if c.StatementCacheMode == StatementCacheModeDescribeExec {
		pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	}

	return pc, nil
}

func nonZeroDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
