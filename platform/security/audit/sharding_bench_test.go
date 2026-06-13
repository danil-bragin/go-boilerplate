package audit_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// TestShardingConsistencyBench measures write-path throughput AND stability for
// the Strong and Eventual consistency models across {1,2,4} Postgres shards
// (gated by TIER3_BENCH=1, needs Docker). It ties Tier-3 (pg.ShardedPool) to A2
// (per-command consistency):
//
//   - Strong: one keyed transaction per order doing the effectively-once write
//     pattern (order row + outbox row) PLUS a synchronous audit-chain write in
//     the SAME tx (can't-audit → don't-commit). The audit chain-head FOR UPDATE
//     is the per-shard serialization point, so sharding (one chain per shard)
//     is what lets it scale.
//   - Eventual: a single autocommit insert per order + a non-blocking enqueue to
//     a per-shard buffered audit writer (best-effort, batched off the hot path).
//     The command never waits on the audit.
//
// Stability is asserted, not just throughput: every order + outbox row lands,
// the audit is fully accounted (Strong: every row in a chain; Eventual:
// drained+dropped == total), and every shard's hash chain passes VerifyChain
// (tamper-evidence holds under concurrent sharded load).
func TestShardingConsistencyBench(t *testing.T) {
	if os.Getenv("TIER3_BENCH") != "1" {
		t.Skip("set TIER3_BENCH=1 to run the strong/eventual sharding throughput+stability bench (needs Docker)")
	}

	const (
		strongTotal   = 4000  // chain-serialized ⇒ ~seconds per shard count
		eventualTotal = 40000 // autocommit ⇒ need a big total for a non-noisy window
		conc          = 32
	)
	shardCounts := []int{1, 2, 4}

	type result struct {
		model  string
		shards int
		rate   float64
		extra  string
	}
	var results []result

	for _, n := range shardCounts {
		t.Run(fmt.Sprintf("strong/shards=%d", n), func(t *testing.T) {
			sp := buildShards(t, n) // containers terminate at this subtest's end
			rate := measureStrong(t, sp, strongTotal, conc)
			results = append(results, result{"strong", n, rate, verifyAllShards(t, sp, strongTotal)})
		})
	}
	for _, n := range shardCounts {
		t.Run(fmt.Sprintf("eventual/shards=%d", n), func(t *testing.T) {
			sp := buildShards(t, n)
			rate, extra := measureEventual(t, sp, eventualTotal, conc)
			results = append(results, result{"eventual", n, rate, extra})
		})
	}

	t.Logf("=== Tier-3 × A2 sharding bench (strongTotal=%d eventualTotal=%d, conc=%d, single audit actor) ===", strongTotal, eventualTotal, conc)
	base := map[string]float64{}
	for _, r := range results {
		if r.shards == 1 {
			base[r.model] = r.rate
		}
	}
	for _, r := range results {
		t.Logf("  %-8s shards=%d → %9.0f orders/s   (%.2fx vs 1 shard)   %s",
			r.model, r.shards, r.rate, r.rate/base[r.model], r.extra)
	}
}

// buildShards spins n fresh Postgres containers, applies the audit migrations +
// the bench's orders_t/outbox_t tables on each, and returns a ShardedPool over
// them. Containers terminate when the calling (sub)test ends (pgtest.NewDSN).
func buildShards(t *testing.T, n int) *pg.ShardedPool {
	t.Helper()
	ctx := context.Background()
	dsns := make([]config.Secret, n)
	for i := range n {
		dsn := pgtest.NewDSN(t)
		require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"),
			"audit migrations must apply on shard %d", i)
		dsns[i] = config.Secret(dsn)
	}
	sp, err := pg.NewSharded(ctx, pg.ShardedConfig{DSNs: dsns})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sp.Close(context.Background()) })
	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		if _, err := p.Writer().Exec(ctx, `create table orders_t (id text primary key, amount bigint)`); err != nil {
			return err
		}
		_, err := p.Writer().Exec(ctx, `create table outbox_t (id bigserial primary key, order_id text not null, payload text)`)
		return err
	}))
	return sp
}

// measureStrong runs `total` Strong order-writes at `conc`: each is one keyed
// transaction inserting the order row + an outbox row + a synchronous audit
// entry (the can't-audit-don't-commit pattern), all on the order's shard.
func measureStrong(t *testing.T, sp *pg.ShardedPool, total, conc int) float64 {
	t.Helper()
	store := audit.NewPgStore(sp.Shards()[0]) // routes via the ambient tx to the order's shard
	rate := runConc(total, conc, func(i int) {
		oid := fmt.Sprintf("order-%d", i)
		ctx := pg.WithShardKey(context.Background(), oid)
		_ = sp.RunInTx(ctx, func(ctx context.Context) error {
			db, err := sp.FromContext(ctx)
			if err != nil {
				return err
			}
			if _, err := db.Exec(ctx, `insert into orders_t (id, amount) values ($1, $2)`, oid, 100); err != nil {
				return err
			}
			if _, err := db.Exec(ctx, `insert into outbox_t (order_id, payload) values ($1, $2)`, oid, "{}"); err != nil {
				return err
			}
			return store.Record(ctx, audit.Entry{Actor: "svc", Action: "order:create", Subject: oid})
		})
	})
	require.Equal(t, total, countAll(t, sp, "orders_t"), "every order row must land")
	require.Equal(t, total, countAll(t, sp, "outbox_t"), "every outbox row must land (effectively-once)")
	return rate
}

// measureEventual runs `total` Eventual order-writes at `conc`: each is a single
// autocommit insert + a non-blocking enqueue to that shard's buffered audit
// writer. The command does not wait on the audit.
func measureEventual(t *testing.T, sp *pg.ShardedPool, total, conc int) (float64, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writers := make([]*audit.BufferedAuditWriter, sp.Len())
	for i, p := range sp.Shards() {
		w := audit.NewBufferedAuditWriter(audit.NewPgStore(p), audit.WriterConfig{
			Buffer: total*2 + 1024, BatchSize: 256, FlushInterval: 10 * time.Millisecond,
		})
		writers[i] = w
		go func() { _ = w.Run(ctx) }()
	}
	idxOf := func(oid string) int {
		owner := sp.Resolve(oid)
		for i, p := range sp.Shards() {
			if p == owner {
				return i
			}
		}
		return 0
	}
	rate := runConc(total, conc, func(i int) {
		oid := fmt.Sprintf("order-%d", i)
		p := sp.Resolve(oid)
		_, _ = p.Writer().Exec(context.Background(), `insert into orders_t (id, amount) values ($1, $2)`, oid, 100)
		writers[idxOf(oid)].Enqueue(audit.Entry{Actor: "svc", Action: "order:view", Subject: oid})
	})
	require.Equal(t, total, countAll(t, sp, "orders_t"), "every order row must land (durable immediately)")
	// Drain: wait until every enqueued audit entry is persisted or accounted as dropped.
	require.Eventually(t, func() bool {
		persisted := countAll(t, sp, "audit_log")
		var dropped int64
		for _, w := range writers {
			dropped += w.Dropped()
		}
		return int64(persisted)+dropped >= int64(total)
	}, 60*time.Second, 100*time.Millisecond, "async audit must drain or account drops")
	var dropped int64
	for _, w := range writers {
		dropped += w.Dropped()
	}
	return rate, fmt.Sprintf("audit drained=%d dropped=%d", countAll(t, sp, "audit_log"), dropped)
}

// verifyAllShards asserts every shard's audit hash chain passes VerifyChain and
// the total verified rows equals total (tamper-evidence under sharded load).
func verifyAllShards(t *testing.T, sp *pg.ShardedPool, total int) string {
	t.Helper()
	ctx := context.Background()
	verified := 0
	for i, p := range sp.Shards() {
		store := audit.NewPgStore(p)
		res, err := store.VerifyChain(ctx, time.Time{})
		require.NoError(t, err)
		require.True(t, res.OK, "shard %d chain must verify; break id=%d reason=%q", i, res.BreakID, res.Reason)
		verified += res.Verified
	}
	require.Equal(t, total, verified, "every audit entry must be in a verified chain")
	return fmt.Sprintf("audit verified=%d (all chains OK)", verified)
}

// runConc fires `total` invocations of fn across `conc` goroutines and returns
// the sustained rate (ops/s).
func runConc(total, conc int, fn func(i int)) float64 {
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range total {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
	}
	wg.Wait()
	return float64(total) / time.Since(start).Seconds()
}

// countAll sums row counts of table across every shard.
func countAll(t *testing.T, sp *pg.ShardedPool, table string) int {
	t.Helper()
	ctx := context.Background()
	sum := 0
	for _, p := range sp.Shards() {
		var n int
		require.NoError(t, p.Reader().QueryRow(ctx, `select count(*) from `+table).Scan(&n))
		sum += n
	}
	return sum
}
