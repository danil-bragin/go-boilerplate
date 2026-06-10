package servicekit

import (
	"context"
	"embed"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/migrations/*.sql
var kitTestMigrations embed.FS

// TestConfig_MigrateOnStartDefaultTrue: the env default is true (dev/test
// convenience); production should run a migrate job and set it to false.
func TestConfig_MigrateOnStartDefaultTrue(t *testing.T) {
	cfg, err := config.Load[Config]()
	require.NoError(t, err)
	assert.True(t, cfg.MigrateOnStart, "MIGRATE_ON_START must default to true")
}

func markerTableExists(t *testing.T, dsn string) bool {
	t.Helper()
	ctx := context.Background()
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	defer func() { _ = pool.Close(ctx) }()

	var exists bool
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select exists (select 1 from information_schema.tables where table_name='kit_migrate_marker')`,
	).Scan(&exists))
	return exists
}

func newMigrateService(t *testing.T, migrateOnStart bool) (string, *Service) {
	t.Helper()
	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	cfg := Config{AdminAddr: "127.0.0.1:0", MigrateOnStart: migrateOnStart}
	cfg.PG.DSN = config.Secret(dsn)
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	cfg.InboxCleanupInterval = 0

	svc, err := New(context.Background(), cfg, kitTestMigrations, "testdata/migrations")
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = svc.Stop(ctx)
	})
	return dsn, svc
}

// TestNew_MigrateOnStartTrueApplies: with the default, New applies embedded
// migrations.
func TestNew_MigrateOnStartTrueApplies(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker")
	}
	dsn, _ := newMigrateService(t, true)
	assert.True(t, markerTableExists(t, dsn), "MIGRATE_ON_START=true must apply migrations")
}

// TestNew_MigrateOnStartFalseSkips: MIGRATE_ON_START=false must skip the
// embedded migrations even when an FS is provided (prod: migrate job owns it).
func TestNew_MigrateOnStartFalseSkips(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker")
	}
	dsn, _ := newMigrateService(t, false)
	assert.False(t, markerTableExists(t, dsn), "MIGRATE_ON_START=false must skip migrations")
}
