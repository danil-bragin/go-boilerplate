package app_test

import (
	"context"
	"testing"
	"time"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// gatewayPool boots a fresh Postgres, applies ALL gateway migrations (including
// the new product_views table and the existing audit_log + hash-chain schema),
// and returns a ready pool. Mirrors the integration-test wiring used elsewhere
// in the gateway (pgtest + pg.Migrate over migrations.FS), but self-contained
// in the app package so it needs no app-level HTTP bootstrap.
func gatewayPool(t *testing.T) *pg.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"),
		"gateway migrations (incl. product_views + audit_log) must apply")
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

// TestRecordProductView_EventualAsyncAudit proves the A2 differentiation end to
// end: the demo command runs with Eventual.With(Transactional(false)) — no
// wrapping tx — and its audit row lands asynchronously through the buffered
// writer (best-effort), not inside the command. The single product_views insert
// is durable immediately; the audit_log row appears only after the writer
// drains, which is exactly the relaxed-consistency contract.
func TestRecordProductView_EventualAsyncAudit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}

	pool := gatewayPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := audit.NewPgStore(pool)
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{
		Buffer:        256,
		BatchSize:     32,
		FlushInterval: 20 * time.Millisecond,
	})
	go func() { _ = w.Run(ctx) }()

	h := gatewayapp.DecorateRecordProductView(gatewayapp.RecordProductViewHandler(pool), w)
	_, err := h(ctx, gatewayapp.RecordProductView{ProductID: "p1", Viewer: "u1"})
	require.NoError(t, err)

	// The single local insert is durable the moment the handler returns — no
	// async hop, no transaction to await.
	var views int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from product_views`).Scan(&views))
	require.Equal(t, 1, views)

	// The audit row, by contrast, is best-effort/async: it only appears once the
	// buffered writer drains. This is the whole point of SyncAudit:false.
	require.Eventually(t, func() bool {
		var a int
		_ = pool.Reader().QueryRow(ctx,
			`select count(*) from audit_log where action = 'product:view'`).Scan(&a)
		return a == 1
	}, 5*time.Second, 50*time.Millisecond,
		"async audit row must land after the buffered writer drains")
}
