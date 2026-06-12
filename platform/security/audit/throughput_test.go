package audit_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

// measureAuditThroughput runs `total` audit writes spread across `actors`
// distinct actors with `shards` audit chains, at `conc` concurrency, on a fresh
// DB; it returns the sustained write rate (records/s) and the number of chains
// actually used. This isolates the audit bottleneck — the chain-head FOR UPDATE
// serialization that caps the order-create command path (order/outbox inserts
// are NOT serialized, so audit-write throughput approximates the command
// ceiling).
func measureAuditThroughput(t *testing.T, actors, shards, total, conc int) (float64, int) {
	t.Helper()
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(shards))

	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range total {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			actor := fmt.Sprintf("actor-%d", i%actors)
			_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
				return store.Record(ctx, audit.Entry{
					Actor:   actor,
					Action:  "order:create",
					Subject: fmt.Sprintf("o-%d", i),
				})
			})
		}(i)
	}
	wg.Wait()
	rate := float64(total) / time.Since(start).Seconds()

	var chains int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(distinct chain_id) from audit_log`).Scan(&chains))

	// Tamper-evidence must hold after concurrent sharded writes.
	res, err := store.VerifyChain(ctx, time.Time{})
	require.NoError(t, err)
	require.True(t, res.OK,
		"chain must verify after concurrent load; break id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, total, res.Verified)
	return rate, chains
}

// TestAuditThroughput_ShardingScalesWithActorDiversity proves the re-measure-gate
// claim: the per-actor audit-chain sharding (ADR-0018) scales audit-write
// throughput with the number of distinct hot actors. One actor collapses to one
// chain (the FOR UPDATE serialization ceiling ≈ the measured live-stack ~825/s);
// many actors spread across chains for materially higher throughput. Sharding is
// NOT changed; only the actor diversity of the load. Tamper-evidence
// (VerifyChain) holds in both cases.
func TestAuditThroughput_ShardingScalesWithActorDiversity(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput benchmark needs Docker")
	}
	const (
		total  = 3000
		conc   = 20
		shards = 16
	)
	r1, c1 := measureAuditThroughput(t, 1, shards, total, conc)
	rN, cN := measureAuditThroughput(t, 16, shards, total, conc)

	t.Logf("AUDIT_CHAIN_SHARDS=%d, concurrency=%d, %d writes/run", shards, conc, total)
	t.Logf("   1 hot actor  → %6.0f rec/s across %2d chain(s)", r1, c1)
	t.Logf("  16 hot actors → %6.0f rec/s across %2d chain(s)   (%.1fx vs 1 actor)", rN, cN, rN/r1)

	require.Equal(t, 1, c1, "one actor must collapse to exactly one chain (the serialization point)")
	require.GreaterOrEqual(t, cN, 8, "16 actors should spread across most of the 16 chains")
	require.Greater(t, rN, r1*1.5, "actor diversity must materially raise throughput — sharding works")
}
