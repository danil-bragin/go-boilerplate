package outbox_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/outbox"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRelay_Sharded_DisjointOwnership_PreservesOrder runs N shard relays
// concurrently (each its own leader lock + publisher) and asserts:
//  1. every enqueued row is published exactly once (no loss, no cross-shard dup);
//  2. each aggregate is owned by exactly ONE shard (disjoint partitioning);
//  3. within its shard, an aggregate's events publish in per-aggregate order.
func TestRelay_Sharded_DisjointOwnership_PreservesOrder(t *testing.T) {
	const (
		shards   = 3
		perAgg   = 5
		interval = 30 * time.Millisecond
	)
	pool := newPoolWithSchema(t)
	ctx := context.Background()

	aggs := []string{"agg-a", "agg-b", "agg-c", "agg-d", "agg-e", "agg-f", "agg-g"}
	for _, a := range aggs {
		enqueueSeq(t, pool, a, 0, perAgg)
	}
	total := len(aggs) * perAgg

	cfg := outbox.RelayConfig{BatchSize: 100, PollInterval: interval}
	pubs := make([]*fakeBatchPublisher, shards)
	for i := range shards {
		pubs[i] = &fakeBatchPublisher{}
		relay := outbox.NewRelay(pool, pubs[i], cfg,
			outbox.WithSingleActive(pool.Writer()), outbox.WithShard(i, shards))
		rctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() { _ = relay.Run(rctx) }()
	}

	// 1: all rows published exactly once across the fleet.
	require.Eventually(t, func() bool {
		n := 0
		for _, p := range pubs {
			n += p.count()
		}
		return n == total
	}, 10*time.Second, 20*time.Millisecond, "all rows must be published across shards")
	// Settle: give any (incorrect) extra publishes a window to show up.
	time.Sleep(6 * interval)
	got := 0
	for _, p := range pubs {
		got += p.count()
	}
	require.Equal(t, total, got, "no cross-shard duplicates or loss")

	// 2 + 3: disjoint ownership + per-aggregate order.
	owner := map[string]int{} // aggregate -> shard that published it
	for shard, p := range pubs {
		perAggSeqs := map[string][]int{}
		for _, m := range p.messages() {
			perAggSeqs[m.AggregateID] = append(perAggSeqs[m.AggregateID], mustAtoi(m.Payload))
		}
		for agg, seqs := range perAggSeqs {
			if prev, seen := owner[agg]; seen {
				t.Fatalf("aggregate %q published by shard %d AND %d — sharding must be disjoint", agg, prev, shard)
			}
			owner[agg] = shard
			assert.Equal(t, []int{0, 1, 2, 3, 4}, seqs,
				"aggregate %q events must be in per-aggregate order within its shard", agg)
		}
	}
	assert.Len(t, owner, len(aggs), "every aggregate must be owned by exactly one shard")
}

func mustAtoi(b []byte) int {
	n := 0
	for _, c := range b {
		n = n*10 + int(c-'0')
	}
	return n
}
