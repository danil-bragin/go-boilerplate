package app_test

import (
	"context"
	"fmt"
	"os"
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

// capPool boots a fresh Postgres, applies the gateway migrations, and returns a
// pool sized to maxConns so the sweep can find the PG-bound ceiling rather than
// the default (NumCPU) pool ceiling.
func capPool(t *testing.T, maxConns int32) *pg.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{
		DSN:      config.Secret(dsn),
		MaxConns: maxConns,
		MinConns: maxConns / 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

// TestRecordProductView_EventualCap sweeps the Eventual command path across
// concurrency levels to find its ceiling (gated by A2_CAP=1, needs Docker).
//
// An Eventual command is a single autocommit INSERT plus a non-blocking
// Enqueue, so its ceiling is the single-Postgres commit throughput for a trivial
// one-row write — NOT the audit chain (that ran off the hot path). The buffer is
// sized so the command rate reflects the pure command path; drops are reported
// so we can see when commands outrun the async audit drain.
func TestRecordProductView_EventualCap(t *testing.T) {
	if os.Getenv("A2_CAP") != "1" {
		t.Skip("set A2_CAP=1 to run the A2 Eventual-cap sweep (needs Docker)")
	}

	const (
		maxConns = 96
		total    = 20000
	)
	concs := []int{1, 8, 16, 32, 48, 64, 96}

	pool := capPool(t, maxConns)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := audit.NewPgStore(pool)
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{
		Buffer:        total * 2, // do not gate command rate on the buffer
		BatchSize:     256,
		FlushInterval: 10 * time.Millisecond,
	})
	go func() { _ = w.Run(ctx) }()
	h := gatewayapp.DecorateRecordProductView(gatewayapp.RecordProductViewHandler(pool), w)

	t.Logf("Eventual command-rate sweep — %d cmds/run, pool MaxConns=%d, single actor", total, maxConns)
	var best float64
	var bestConc int
	for _, c := range concs {
		// Reset drop counter visibility by reading before/after.
		before := w.Dropped()
		rate := runViewLoad(t, h, total, c)
		dropped := w.Dropped() - before
		t.Logf("  conc %3d → %9.0f cmd/s   (dropped this run: %d)", c, rate, dropped)
		if rate > best {
			best, bestConc = rate, c
		}
	}
	t.Logf("Eventual command-rate CAP ≈ %.0f cmd/s (knee near conc %d)", best, bestConc)
}

// TestRecordProductView_AuditDrainCap measures the OTHER ceiling: how fast the
// BufferedAuditWriter can persist audit for a SINGLE actor (one chain), which is
// the rate at which best-effort audit drains off the hot path. If the command
// rate sustainably exceeds this, the buffer fills and entries drop. Batched
// per-chain writes (RecordBatchSameChain) amortise the chain-head lock, so this
// is far above the sync in-tx audit rate (~1.5k/s measured) — and AUDIT_CHAIN
// sharding multiplies it by actor diversity.
func TestRecordProductView_AuditDrainCap(t *testing.T) {
	if os.Getenv("A2_CAP") != "1" {
		t.Skip("set A2_CAP=1 to run the A2 audit-drain-cap measurement (needs Docker)")
	}

	const enqueue = 50000

	for _, actors := range []int{1, 8, 16, 32} {
		pool := capPool(t, 64)
		ctx, cancel := context.WithCancel(context.Background())
		store := audit.NewPgStore(pool, audit.WithChainShards(actors))
		w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{
			Buffer:        enqueue + 1024,
			BatchSize:     256,
			FlushInterval: 10 * time.Millisecond,
		})
		// Fill the buffer BEFORE the drainer starts so we time pure drain rate.
		for i := range enqueue {
			w.Enqueue(audit.Entry{
				Actor:   fmt.Sprintf("actor-%d", i%actors),
				Action:  "product:view",
				Subject: fmt.Sprintf("p-%d", i),
			})
		}
		start := time.Now()
		go func() { _ = w.Run(ctx) }()
		require.Eventually(t, func() bool {
			var n int
			_ = pool.Reader().QueryRow(ctx, `select count(*) from audit_log`).Scan(&n)
			return n >= enqueue
		}, 120*time.Second, 50*time.Millisecond, "drainer must persist all enqueued entries")
		rate := float64(enqueue) / time.Since(start).Seconds()

		res, err := store.VerifyChain(ctx, time.Time{})
		require.NoError(t, err)
		require.True(t, res.OK, "async-drained chain(s) must verify; break id=%d reason=%q", res.BreakID, res.Reason)

		t.Logf("audit-drain CAP — %d actors / %d chains → %9.0f audit rows/s (VerifyChain OK)", actors, actors, rate)
		cancel()
	}
}
