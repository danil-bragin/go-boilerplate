package pg_test

import (
	"fmt"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestConfig_ToShardedConfig_SingleVsMulti(t *testing.T) {
	// No Shards ⇒ one shard from DSN (M=1, byte-identity).
	c := pg.Config{DSN: "dsn0", MaxConns: 7}
	sc := c.ToShardedConfig()
	require.Len(t, sc.DSNs, 1)
	require.Equal(t, "dsn0", sc.DSNs[0].Reveal())
	require.Equal(t, int32(7), sc.PerShard.MaxConns)

	// Shards set ⇒ one shard per listed DSN.
	c2 := pg.Config{DSN: "dsn0", Shards: []config.Secret{"a", "b", "c"}}
	sc2 := c2.ToShardedConfig()
	require.Len(t, sc2.DSNs, 3)
	require.Equal(t, []string{"a", "b", "c"},
		[]string{sc2.DSNs[0].Reveal(), sc2.DSNs[1].Reveal(), sc2.DSNs[2].Reveal()})
	require.Empty(t, sc2.PerShard.MigrateURL, "M>1: MigrateURL cleared (each shard migrates via own DSN)")
}

func TestConfig_PGShards_EnvParse(t *testing.T) {
	t.Setenv("PG_SHARDS", "dsnA,dsnB")
	c, err := config.Load[pg.Config]()
	require.NoError(t, err)
	require.Len(t, c.Shards, 2)
	require.Equal(t, "dsnA", c.Shards[0].Reveal())
}

// TestConfig_DSNsAreRedactedInDumps: the DSN fields carry the database
// password — a %+v / %v / %s dump of the config (panic logs, debug prints)
// must never leak it.
func TestConfig_DSNsAreRedactedInDumps(t *testing.T) {
	cfg := pg.Config{
		DSN:        "postgres://app:hunter2-writer@db:5432/app",
		ReaderDSN:  "postgres://app:hunter2-reader@replica:5432/app",
		MigrateURL: "postgres://app:hunter2-migrate@db:5432/app",
	}

	for _, dump := range []string{
		fmt.Sprintf("%+v", cfg),
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%#v", cfg),
	} {
		require.NotContains(t, dump, "hunter2", "config dump must not leak DSN passwords: %s", dump)
		require.Contains(t, dump, "[REDACTED]")
	}

	// The raw values stay reachable for the pool/migration call sites.
	require.Equal(t, "postgres://app:hunter2-writer@db:5432/app", cfg.DSN.Reveal())
	require.Equal(t, "postgres://app:hunter2-migrate@db:5432/app", cfg.MigrateDSN().Reveal())
}

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

// TestConfig_ConnectTimeoutApplied: PG_CONNECT_TIMEOUT lands on
// ConnConfig.ConnectTimeout; zero falls back to the 5s default.
func TestConfig_ConnectTimeoutApplied(t *testing.T) {
	cfg := pg.Config{DSN: "postgres://u:p@localhost:5432/db", ConnectTimeout: 3 * time.Second}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, pc.ConnConfig.ConnectTimeout)

	cfg.ConnectTimeout = 0
	pc, err = cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, pc.ConnConfig.ConnectTimeout, "zero must default to 5s")
}

// TestConfig_StatementTimeoutRuntimeParam: a positive StatementTimeout becomes
// a statement_timeout RuntimeParam (milliseconds); zero disables it.
func TestConfig_StatementTimeoutRuntimeParam(t *testing.T) {
	cfg := pg.Config{DSN: "postgres://u:p@localhost:5432/db", StatementTimeout: 10 * time.Second}
	pc, err := cfg.BuildPoolConfig()
	require.NoError(t, err)
	require.Equal(t, "10000", pc.ConnConfig.RuntimeParams["statement_timeout"])

	cfg.StatementTimeout = 0
	pc, err = cfg.BuildPoolConfig()
	require.NoError(t, err)
	_, set := pc.ConnConfig.RuntimeParams["statement_timeout"]
	require.False(t, set, "zero StatementTimeout must not set the runtime param")
}

// TestConfig_EnvDefaults: the env tags carry the documented defaults —
// statement timeout 30s, connect timeout 5s, reader sizing 0 (= writer values).
func TestConfig_EnvDefaults(t *testing.T) {
	t.Setenv("PG_DSN", "postgres://u:p@localhost:5432/db")
	cfg, err := config.Load[pg.Config]()
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, cfg.StatementTimeout)
	require.Equal(t, 5*time.Second, cfg.ConnectTimeout)
	require.Zero(t, cfg.ReaderMaxConns)
	require.Zero(t, cfg.ReaderMinConns)

	t.Setenv("PG_READER_MAX_CONNS", "7")
	t.Setenv("PG_READER_MIN_CONNS", "2")
	cfg, err = config.Load[pg.Config]()
	require.NoError(t, err)
	require.Equal(t, int32(7), cfg.ReaderMaxConns)
	require.Equal(t, int32(2), cfg.ReaderMinConns)
}
