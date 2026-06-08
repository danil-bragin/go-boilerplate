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
	DSN               string        `env:"PG_DSN" envDefault:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
	ReaderDSN         string        `env:"PG_READER_DSN" envDefault:""`
	MaxConns          int32         `env:"PG_MAX_CONNS" envDefault:"10"`
	MinConns          int32         `env:"PG_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime   time.Duration `env:"PG_MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime   time.Duration `env:"PG_MAX_CONN_IDLE_TIME" envDefault:"5m"`
	HealthCheckPeriod time.Duration `env:"PG_HEALTH_CHECK_PERIOD" envDefault:"1m"`
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
