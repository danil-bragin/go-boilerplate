package api

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// twoShardServer boots TWO migrated Postgres shards, wires a Server over the
// ShardedPool with auth disabled, and returns the server plus the shard pools
// for seeding/tampering. Audit verify fans out over both shards.
func twoShardServer(t *testing.T) (*Server, *pg.ShardedPool) {
	t.Helper()
	ctx := context.Background()
	dsns := []config.Secret{
		config.Secret(pgtest.NewDSN(t)),
		config.Secret(pgtest.NewDSN(t)),
	}
	for _, dsn := range dsns {
		require.NoError(t, pg.Migrate(ctx, dsn.Reveal(), migrations.FS, "sql"))
	}
	sp, err := pg.NewSharded(ctx, pg.ShardedConfig{DSNs: dsns})
	require.NoError(t, err)
	require.Equal(t, 2, sp.Len())
	t.Cleanup(func() { _ = sp.Close(context.Background()) })

	s := NewServer(sp, nil, "test-topic", slog.Default(), nil, nil, true /*authDisabled*/)
	return s, sp
}

// seedAuditRows writes a few audit rows onto one shard's pool via a per-shard
// store (RecordOutOfBand manages its own tx + chain head), building a verifiable
// chain on that shard.
func seedAuditRows(t *testing.T, pool *pg.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	store := audit.NewPgStore(pool)
	for i := range n {
		require.NoError(t, store.RecordOutOfBand(ctx, audit.Entry{
			Actor:   "admin",
			Action:  "order:read",
			Subject: "order-" + string(rune('a'+i)),
		}))
	}
}

// TestVerifyAudit_TwoShards_CleanAllOK: with verifiable chains on both shards,
// the fan-out reports OK and Verified sums the rows walked across shards.
func TestVerifyAudit_TwoShards_CleanAllOK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}
	s, sp := twoShardServer(t)
	seedAuditRows(t, sp.Shards()[0], 3)
	seedAuditRows(t, sp.Shards()[1], 4)

	resp, err := s.VerifyAudit(context.Background(), VerifyAuditRequestObject{})
	require.NoError(t, err)
	out, ok := resp.(VerifyAudit200JSONResponse)
	require.True(t, ok)
	require.True(t, out.Ok, "both shard chains must verify")
	require.Equal(t, 7, out.Verified, "Verified must sum rows across both shards")
	require.Nil(t, out.BreakId)
	require.Nil(t, out.Reason)
}

// TestVerifyAudit_TwoShards_TamperReportedWithShardIndex: tampering a row on
// ONE shard breaks only that shard; the fan-out reports !OK and the Reason
// carries the shard index, while the untampered shard is still walked.
func TestVerifyAudit_TwoShards_TamperReportedWithShardIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}
	s, sp := twoShardServer(t)
	seedAuditRows(t, sp.Shards()[0], 3)
	seedAuditRows(t, sp.Shards()[1], 3)

	// Tamper a historical row on shard 1 (test role owns the table, so the
	// append-only REVOKE is a no-op here).
	ctx := context.Background()
	_, err := sp.Shards()[1].Writer().Exec(ctx,
		`update audit_log set subject = 'HACKED' where id = (select min(id) from audit_log)`)
	require.NoError(t, err)

	resp, err := s.VerifyAudit(ctx, VerifyAuditRequestObject{})
	require.NoError(t, err)
	out, ok := resp.(VerifyAudit200JSONResponse)
	require.True(t, ok)
	require.False(t, out.Ok, "a tamper on any shard must fail the aggregate verify")
	require.NotNil(t, out.Reason)
	require.Contains(t, *out.Reason, "shard 1", "the break reason must name the tampered shard")
	require.Contains(t, *out.Reason, "entry_hash mismatch")
	require.NotNil(t, out.BreakId)
}
