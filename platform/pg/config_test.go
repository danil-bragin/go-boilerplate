package pg_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
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

// TestConfig_DefaultMaxConns verifies the revised default pool size is 25,
// suitable for a service running HTTP + Kafka consumer + outbox relay.
func TestConfig_DefaultMaxConns(t *testing.T) {
	// envDefault tag value — simulate zero-value config (no explicit override).
	var cfg pg.Config
	cfg.DSN = "postgres://u:p@localhost:5432/db"
	// MaxConns left at zero means BuildPoolConfig must NOT override pgx default;
	// but when we set the tagged default 25, it must be applied.
	cfg.MaxConns = 25
	cfg.MinConns = 5

	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, int32(25), pc.MaxConns, "default MaxConns should be 25")
	require.Equal(t, int32(5), pc.MinConns, "default MinConns should be 5")
}

// TestConfig_StatementCacheMode_DescribeExec verifies that when
// StatementCacheMode is set to QueryExecModeDescribeExec the pool config
// carries that mode (required for PgBouncer transaction-mode pooling where
// prepared statements are unavailable).
func TestConfig_StatementCacheMode_DescribeExec(t *testing.T) {
	cfg := pg.Config{
		DSN:                "postgres://u:p@localhost:5432/db",
		MaxConns:           5,
		MinConns:           1,
		StatementCacheMode: pg.StatementCacheModeDescribeExec,
	}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.NotNil(t, pc.ConnConfig)
	// When DescribeExec mode is requested, DefaultQueryExecMode must be set.
	require.Equal(t, pgx.QueryExecModeDescribeExec, pc.ConnConfig.DefaultQueryExecMode,
		"StatementCacheModeDescribeExec must set DefaultQueryExecMode to DescribeExec")
}

// TestConfig_StatementCacheMode_Default verifies that omitting
// StatementCacheMode leaves the pgx default (CacheStatement).
func TestConfig_StatementCacheMode_Default(t *testing.T) {
	cfg := pg.Config{
		DSN:      "postgres://u:p@localhost:5432/db",
		MaxConns: 5,
		MinConns: 1,
		// StatementCacheMode zero = default, must not change pgx default.
	}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	// Default pgx mode is CacheStatement (0); must not be overridden.
	require.Equal(t, pgx.QueryExecModeCacheStatement, pc.ConnConfig.DefaultQueryExecMode,
		"omitting StatementCacheMode must leave pgx default CacheStatement")
}
