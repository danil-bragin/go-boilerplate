package app_test

import (
	"context"
	"sort"
	"testing"
	"time"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// twoShardPool boots TWO fresh Postgres containers, migrates each, and returns
// a 2-shard ShardedPool over them. Mirrors newShardedTestPool in the pg package
// but applies the gateway migrations (orders_read + audit schema) to each shard.
func twoShardPool(t *testing.T) *pg.ShardedPool {
	t.Helper()
	ctx := context.Background()
	dsns := []config.Secret{
		config.Secret(pgtest.NewDSN(t)),
		config.Secret(pgtest.NewDSN(t)),
	}
	for _, dsn := range dsns {
		require.NoError(t, pg.Migrate(ctx, dsn.Reveal(), migrations.FS, "sql"),
			"gateway migrations must apply to each shard")
	}
	sp, err := pg.NewSharded(ctx, pg.ShardedConfig{DSNs: dsns})
	require.NoError(t, err)
	require.Equal(t, 2, sp.Len())
	t.Cleanup(func() { _ = sp.Close(context.Background()) })
	return sp
}

// seedOrder inserts one orders_read row on the shard that OWNS its order id
// (sp.Resolve) — exactly where the sharded projection would write it — at the
// given created_at, returning the id. amountCents carries the customer id when
// scoped tests need it.
func seedOrder(t *testing.T, sp *pg.ShardedPool, id uuid.UUID, customer string, createdAt time.Time) {
	t.Helper()
	ctx := context.Background()
	shard := sp.Resolve(id.String())
	_, err := shard.Writer().Exec(ctx,
		`insert into orders_read (order_id, customer_id, amount_cents, currency, status, created_at, updated_at)
		 values ($1,$2,1,'USD','created',$3,$3)`,
		id, customer, createdAt.UTC())
	require.NoError(t, err)
}

type wantRow struct {
	id        uuid.UUID
	createdAt time.Time
}

// drainList pages through the handler with the given pageSize and returns the
// emitted ids in order, asserting no page exceeds pageSize and the run
// terminates.
func drainList(t *testing.T, h func(context.Context, gatewayapp.ListOrders) (gatewayapp.OrderPage, error), customer string, pageSize int) []string {
	t.Helper()
	ctx := context.Background()
	var got []string
	cursor := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 100, "pagination must terminate")
		page, err := h(ctx, gatewayapp.ListOrders{Cursor: cursor, Limit: pageSize, CustomerID: customer})
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Items), pageSize, "a page must not exceed pageSize")
		for _, it := range page.Items {
			got = append(got, it.OrderID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return got
}

// globalOrder returns the ids of want sorted by (created_at DESC, order_id DESC)
// — the listing order the sharded merge must reproduce.
func globalOrder(want []wantRow) []string {
	sorted := append([]wantRow(nil), want...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].createdAt.Equal(sorted[j].createdAt) {
			return sorted[i].createdAt.After(sorted[j].createdAt)
		}
		return sorted[i].id.String() > sorted[j].id.String()
	})
	ids := make([]string, len(sorted))
	for i, r := range sorted {
		ids[i] = r.id.String()
	}
	return ids
}

// TestListOrders_TwoShards_ScatterGatherMerge proves the M>1 LIST is a correct
// global keyset listing: orders are spread across two physical shards, and the
// handler's paginated output equals the globally (created_at, order_id)-ordered
// sequence with NO duplicates and NO gaps across page boundaries.
func TestListOrders_TwoShards_ScatterGatherMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}
	sp := twoShardPool(t)
	h := gatewayapp.ListOrdersHandler(sp)

	// 40 orders with interleaved timestamps; some share a created_at so the
	// order_id tiebreak is exercised. Spread lands on both shards by id hash.
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	const total = 40
	want := make([]wantRow, 0, total)
	bothShardsSeen := map[int]int{}
	for i := range total {
		id := uuid.New()
		// Collide every 4th row's timestamp with its predecessor.
		ts := base.Add(time.Duration(i/2) * time.Second)
		seedOrder(t, sp, id, "alice", ts)
		want = append(want, wantRow{id: id, createdAt: ts})
		if sp.Resolve(id.String()) == sp.Shards()[0] {
			bothShardsSeen[0]++
		} else {
			bothShardsSeen[1]++
		}
	}
	require.Greater(t, bothShardsSeen[0], 0, "test must place rows on shard 0")
	require.Greater(t, bothShardsSeen[1], 0, "test must place rows on shard 1")

	expected := globalOrder(want)

	// Page through with a size that does not divide the total — exercises the
	// trailing partial page and the per-shard cursor advance.
	got := drainList(t, h, "", 7)
	require.Equal(t, expected, got, "merged sharded listing must equal the global (created_at,id) order")

	// No duplicates.
	seen := map[string]bool{}
	for _, id := range got {
		require.False(t, seen[id], "id %s emitted twice", id)
		seen[id] = true
	}
	require.Len(t, got, total, "every seeded order must appear exactly once")
}

// TestListOrders_TwoShards_CursorResumeNoDupGap pins the page-boundary
// contract: the first page of N equals the global first N, and resuming with
// its cursor yields exactly the global next N (no row repeated, none skipped).
func TestListOrders_TwoShards_CursorResumeNoDupGap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}
	sp := twoShardPool(t)
	h := gatewayapp.ListOrdersHandler(sp)
	ctx := context.Background()

	base := time.Date(2026, 6, 2, 9, 0, 0, 0, time.UTC)
	const total = 25
	want := make([]wantRow, 0, total)
	for i := range total {
		id := uuid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		seedOrder(t, sp, id, "bob", ts)
		want = append(want, wantRow{id: id, createdAt: ts})
	}
	expected := globalOrder(want)

	const pageSize = 10
	p1, err := h(ctx, gatewayapp.ListOrders{Limit: pageSize})
	require.NoError(t, err)
	require.Len(t, p1.Items, pageSize)
	require.NotEmpty(t, p1.NextCursor, "a full first page must carry a resume cursor")
	got1 := idsOf(p1)
	require.Equal(t, expected[:pageSize], got1, "first page must equal global first N")

	p2, err := h(ctx, gatewayapp.ListOrders{Cursor: p1.NextCursor, Limit: pageSize})
	require.NoError(t, err)
	require.Len(t, p2.Items, pageSize)
	got2 := idsOf(p2)
	require.Equal(t, expected[pageSize:2*pageSize], got2, "second page must equal global next N (no dup/gap)")

	// No overlap between the two pages.
	for _, id := range got2 {
		require.NotContains(t, got1, id, "id %s appeared on both pages", id)
	}
}

// TestListOrders_TwoShards_MatchesSingleShard cross-checks the sharded merge
// against the SAME rows loaded into a single shard: the two listings must be
// identical, proving sharding changes only the storage layout, not the result.
func TestListOrders_TwoShards_MatchesSingleShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode (needs Docker)")
	}
	two := twoShardPool(t)
	one := pg.WrapPool(singleShardPool(t))

	base := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	ids := make([]uuid.UUID, 0, 30)
	for i := range 30 {
		id := uuid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		ids = append(ids, id)
		// Same logical row, same created_at, in both layouts.
		seedOrder(t, two, id, "carol", ts)
		seedOrderInto(t, one.Shards()[0], id, "carol", ts)
	}

	twoOut := drainList(t, gatewayapp.ListOrdersHandler(two), "", 8)
	oneOut := drainList(t, gatewayapp.ListOrdersHandler(one), "", 8)
	require.Equal(t, oneOut, twoOut, "sharded listing must match the single-shard listing of the same rows")
	require.Len(t, twoOut, len(ids))
}

func idsOf(p gatewayapp.OrderPage) []string {
	out := make([]string, len(p.Items))
	for i, it := range p.Items {
		out[i] = it.OrderID
	}
	return out
}

// singleShardPool boots one migrated Postgres and returns its pool.
func singleShardPool(t *testing.T) *pg.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

// seedOrderInto inserts directly into one pool (no shard routing).
func seedOrderInto(t *testing.T, pool *pg.Pool, id uuid.UUID, customer string, createdAt time.Time) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Writer().Exec(ctx,
		`insert into orders_read (order_id, customer_id, amount_cents, currency, status, created_at, updated_at)
		 values ($1,$2,1,'USD','created',$3,$3)`,
		id, customer, createdAt.UTC())
	require.NoError(t, err)
}
