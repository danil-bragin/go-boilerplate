package audit_test

import (
	"context"
	"strconv"
	"testing"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

func recordActors(t *testing.T, pool *pg.Pool, store *audit.PgStore, actors []string, rounds int) {
	t.Helper()
	ctx := context.Background()
	for r := range rounds {
		for _, a := range actors {
			require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
				return store.Record(ctx, audit.Entry{
					Actor:    a,
					Action:   "order:create",
					Subject:  "order-" + strconv.Itoa(r),
					Metadata: map[string]string{"seq": strconv.Itoa(r)},
				})
			}))
		}
	}
}

// TestVerifyChain_Sharded_IndependentChains: with N>1 chains, writes spread
// across multiple chain heads, each actor maps to exactly one chain, and the
// whole set verifies (every chain anchored independently).
func TestVerifyChain_Sharded_IndependentChains(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(4))

	actors := []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace"}
	const rounds = 5
	recordActors(t, pool, store, actors, rounds)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK, "sharded chains must verify; break id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, len(actors)*rounds, res.Verified, "every row across all chains is walked")

	var chains int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_chain_head`).Scan(&chains))
	require.Greater(t, chains, 1, "sharding must populate more than one chain head")

	// Each actor's rows must all share ONE chain_id (per-actor chain is stable).
	rows, err := pool.Reader().Query(ctx,
		`select actor, count(distinct chain_id) from audit_log group by actor`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var actor string
		var distinct int
		require.NoError(t, rows.Scan(&actor, &distinct))
		require.Equal(t, 1, distinct, "actor %q must map to exactly one chain", actor)
	}
	require.NoError(t, rows.Err())
}

// TestVerifyChain_Sharded_DetectsTamperInOneChain: an in-place edit of a row in
// one shard chain breaks that chain; VerifyChain (which walks every chain) flags
// it even though the other chains are clean.
func TestVerifyChain_Sharded_DetectsTamperInOneChain(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(4))
	recordActors(t, pool, store, []string{"alice", "bob", "carol", "dave"}, 4)

	// Superuser UPDATE (test role owns the table, so the append-only REVOKE is a
	// no-op here) — tamper one historical row.
	_, err := pool.Writer().Exec(ctx,
		`update audit_log set subject = 'HACKED' where id = (select min(id) from audit_log)`)
	require.NoError(t, err)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "tamper inside a sharded chain must be detected")
	require.Equal(t, "entry_hash mismatch", res.Reason)
}

// TestVerifyChain_Sharded_DefaultIsSingleChain: WithChainShards(1) (and the
// default) keep everything on chain 1 — the unsharded behaviour.
func TestVerifyChain_Sharded_DefaultIsSingleChain(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(1)) // <=1 ⇒ single chain
	recordActors(t, pool, store, []string{"alice", "bob", "carol"}, 4)

	var chains int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(distinct chain_id) from audit_log`).Scan(&chains))
	require.Equal(t, 1, chains, "default/shards<=1 must use a single chain")

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK)
}
