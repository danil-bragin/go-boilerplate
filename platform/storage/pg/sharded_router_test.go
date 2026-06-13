package pg_test

import (
	"fmt"
	"testing"

	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

func TestRouter_DeterministicAcrossProcesses(t *testing.T) {
	r := pg.NewRouter(4)
	keys := []string{"order-1", "order-2", "order-3", "order-abc", "order-xyz"}
	got := map[string]int{}
	for _, k := range keys {
		got[k] = r.Physical(k)
	}
	// stable within process; same key → same shard
	for _, k := range keys {
		require.Equal(t, got[k], r.Physical(k))
	}
	// all within range
	for _, v := range got {
		require.GreaterOrEqual(t, v, 0)
		require.Less(t, v, 4)
	}

	// FROZEN determinism table — FNV-1a 64-bit, 256 logical shards, 4 physical shards.
	// These values MUST NOT change; a change signals a hash algorithm or parameter
	// regression that would break cross-process routing in production.
	// Observed: logical shard shown in comment; physical = logical % 4.
	require.Equal(t, 1, r.Physical("order-1"), "order-1: logical=109 → physical=1")
	require.Equal(t, 0, r.Physical("order-2"), "order-2: logical=84  → physical=0")
	require.Equal(t, 3, r.Physical("order-3"), "order-3: logical=7   → physical=3")
	require.Equal(t, 2, r.Physical("order-abc"), "order-abc: logical=10 → physical=2")
	require.Equal(t, 1, r.Physical("order-xyz"), "order-xyz: logical=97 → physical=1")
}

func TestRouter_SingleShardIdentity(t *testing.T) {
	r := pg.NewRouter(1)
	for _, k := range []string{"a", "b", "c", "anything"} {
		require.Equal(t, 0, r.Physical(k), "M=1 ⇒ every key resolves to shard 0")
	}
}

func TestRouter_SpreadsAcrossShards(t *testing.T) {
	r := pg.NewRouter(4)
	seen := map[int]int{}
	for i := range 1000 {
		seen[r.Physical(fmt.Sprintf("order-%d", i))]++
	}
	require.Len(t, seen, 4, "1000 keys should touch all 4 shards")
	for shard, n := range seen {
		require.Greater(t, n, 100, "shard %d got too few keys (%d) — distribution skew", shard, n)
	}
}
