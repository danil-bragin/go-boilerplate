package consume_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// newShardedInboxPool spins n fresh Postgres containers (one per shard), creates
// the inbox table on each, and returns the ShardedPool. Each shard is a real
// self-cleaning container — no mocks.
func newShardedInboxPool(t *testing.T, n int) *pg.ShardedPool {
	t.Helper()
	ctx := context.Background()
	dsns := make([]config.Secret, n)
	for i := range n {
		dsns[i] = config.Secret(pgtest.NewDSN(t))
	}
	sp, err := pg.NewSharded(ctx, pg.ShardedConfig{DSNs: dsns})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sp.Close(context.Background()) })
	for _, p := range sp.Shards() {
		_, err := p.Writer().Exec(ctx, `
			create table inbox (
				consumer     text not null,
				message_id   text not null,
				processed_at timestamptz not null default now(),
				primary key (consumer, message_id)
			)`)
		require.NoError(t, err)
	}
	return sp
}

// orderRecordForKey builds an OrderCreated record whose Kafka key (the shard
// key) is key and whose message-id is msgID.
func orderRecordForKey(t *testing.T, key, msgID string) kafka.Record {
	t.Helper()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: key, CustomerId: "c1", AmountCents: 100, Currency: "USD",
	})
	require.NoError(t, err)
	return kafka.Record{
		Topic:   "orders.events",
		Key:     []byte(key),
		Value:   payload,
		Headers: map[string]string{"event-type": "orders.OrderCreated.v1", "message-id": msgID},
	}
}

// inboxRowCount returns how many inbox rows the given shard pool holds for
// (consumer, message_id).
func inboxRowCount(t *testing.T, p *pg.Pool, consumer, msgID string) int {
	t.Helper()
	var n int
	require.NoError(t, p.Reader().QueryRow(context.Background(),
		`select count(*) from inbox where consumer = $1 and message_id = $2`, consumer, msgID).Scan(&n))
	return n
}

// twoKeysOnDifferentShards returns two keys that the 2-shard router resolves to
// different physical shards, so the routing assertion is meaningful.
func twoKeysOnDifferentShards(t *testing.T, sp *pg.ShardedPool) (a, b string) {
	t.Helper()
	require.Equal(t, 2, sp.Len())
	keyA := "key-0"
	shardA := sp.Resolve(keyA)
	for i := 1; i < 10000; i++ {
		cand := "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26%10)) + string(rune('0'+i%10))
		if sp.Resolve(cand) != shardA {
			return keyA, cand
		}
	}
	t.Fatal("could not find two keys on different shards")
	return "", ""
}

// TestProcessRecord_RoutesInboxRowToResolvedShard proves Task B3: each consumed
// record's inbox dedup row (and therefore its handler side effect, which runs in
// the same shard tx) lands on sp.Resolve(string(r.Key)) ONLY — never on the
// other shard. Two records whose keys resolve to DIFFERENT shards each commit on
// their own shard. Real testcontainers, real inbox.ProcessOnce, stub handler.
func TestProcessRecord_RoutesInboxRowToResolvedShard(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (2 postgres containers)")
	}
	t.Parallel()
	ctx := context.Background()
	sp := newShardedInboxPool(t, 2)

	const group = "shard-routing"
	keyA, keyB := twoKeysOnDifferentShards(t, sp)
	shardA, shardB := sp.Resolve(keyA), sp.Resolve(keyB)
	require.NotEqual(t, shardA, shardB, "test keys must resolve to different shards")

	var handled []string
	h := consume.New(sp, group).Handler(
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			handled = append(handled, evt.GetOrderId())
			return nil
		}),
	)

	require.NoError(t, h(ctx, orderRecordForKey(t, keyA, "mA")))
	require.NoError(t, h(ctx, orderRecordForKey(t, keyB, "mB")))

	require.ElementsMatch(t, []string{keyA, keyB}, handled, "both records handled exactly once")

	// Record A's inbox row is on shard A only; record B's on shard B only.
	require.Equal(t, 1, inboxRowCount(t, shardA, group, "mA"), "mA must dedup-row on its resolved shard")
	require.Equal(t, 0, inboxRowCount(t, shardB, group, "mA"), "mA must NOT touch the other shard")
	require.Equal(t, 1, inboxRowCount(t, shardB, group, "mB"), "mB must dedup-row on its resolved shard")
	require.Equal(t, 0, inboxRowCount(t, shardA, group, "mB"), "mB must NOT touch the other shard")

	// Redelivery of A is deduped on its shard (handler not called again).
	require.NoError(t, h(ctx, orderRecordForKey(t, keyA, "mA")))
	require.ElementsMatch(t, []string{keyA, keyB}, handled, "duplicate must be deduped on the resolved shard")
}

// TestBatchHandler_RoutesAcrossShards proves the batch path groups records BY
// SHARD and commits each shard-group's batch tx on that shard's pool. A poll
// batch spanning two shards lands each record's inbox row on its resolved shard.
func TestBatchHandler_RoutesAcrossShards(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (2 postgres containers)")
	}
	t.Parallel()
	ctx := context.Background()
	sp := newShardedInboxPool(t, 2)

	const group = "shard-routing-batch"
	keyA, keyB := twoKeysOnDifferentShards(t, sp)
	shardA, shardB := sp.Resolve(keyA), sp.Resolve(keyB)

	var handled []string
	bh := consume.New(sp, group).BatchHandler(
		nil,
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			handled = append(handled, evt.GetOrderId())
			return nil
		}),
	)

	// One poll batch spanning both shards.
	batch := []kafka.Record{
		orderRecordForKey(t, keyA, "bA"),
		orderRecordForKey(t, keyB, "bB"),
	}
	n, err := bh(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, len(batch), n)
	require.ElementsMatch(t, []string{keyA, keyB}, handled)

	require.Equal(t, 1, inboxRowCount(t, shardA, group, "bA"))
	require.Equal(t, 0, inboxRowCount(t, shardB, group, "bA"))
	require.Equal(t, 1, inboxRowCount(t, shardB, group, "bB"))
	require.Equal(t, 0, inboxRowCount(t, shardA, group, "bB"))
}
