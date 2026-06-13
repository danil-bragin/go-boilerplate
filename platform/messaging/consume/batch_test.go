package consume_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// orderRecordWithID builds an OrderCreated record carrying the given order id,
// message id, partition, and offset, with the standard event-type header.
func orderRecordWithID(t *testing.T, orderID, msgID string, partition int32, offset int64) kafka.Record {
	t.Helper()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: orderID, CustomerId: "c1", AmountCents: 100, Currency: "USD",
	})
	require.NoError(t, err)
	return kafka.Record{
		Topic:     "orders.events",
		Key:       []byte(orderID),
		Value:     payload,
		Headers:   map[string]string{"event-type": "orders.OrderCreated.v1", "message-id": msgID},
		Partition: partition,
		Offset:    offset,
	}
}

// inboxCount returns how many inbox rows exist for the given consumer group.
func inboxCount(t *testing.T, pool *pg.Pool, group string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select count(*) from inbox where consumer = $1`, group).Scan(&n))
	return n
}

// recorder collects the order ids the handler applied, in call order, under a
// mutex (the batch path runs in one goroutine, but be safe for the fallback).
type recorder struct {
	mu        sync.Mutex
	applied   []string
	committed []string
}

func (r *recorder) record(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, id)
}

func (r *recorder) commit(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = append(r.committed, id)
}

func (r *recorder) appliedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.applied...)
}

func (r *recorder) committedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.committed...)
}

// TestBatchHandler_AppliesCleanBatchInOneTx — a clean batch of N applies all
// side effects in offset order, and OnCommitted fires per-record in order
// after the (single) commit.
func TestBatchHandler_AppliesCleanBatchInOneTx(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	rec := &recorder{}
	h := consume.New(pg.WrapPool(pool), "grp-batch-clean").BatchHandler(
		nil, // fallback defaults to processRecord
		consume.Typed(
			"orders.OrderCreated.v1",
			func(ctx context.Context, evt *ordersv1.OrderCreated) error {
				// Inside the single tx, none of the inbox rows are visible to
				// other connections yet (proves one tx, not per-record).
				var n int
				require.NoError(t, pool.Reader().QueryRow(ctx,
					`select count(*) from inbox where consumer = 'grp-batch-clean'`).Scan(&n))
				require.Zero(t, n, "batch must run in one uncommitted tx")
				rec.record(evt.GetOrderId())
				return nil
			},
			consume.OnCommitted(func(_ context.Context, evt *ordersv1.OrderCreated) {
				rec.commit(evt.GetOrderId())
			}),
		),
	)

	records := []kafka.Record{
		orderRecordWithID(t, "a", "m-a", 0, 1),
		orderRecordWithID(t, "b", "m-b", 0, 2),
		orderRecordWithID(t, "c", "m-c", 0, 3),
	}

	processed, err := h(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, len(records), processed, "clean batch fully acked")
	assert.Equal(t, []string{"a", "b", "c"}, rec.appliedIDs(), "applied in offset order")
	assert.Equal(t, []string{"a", "b", "c"}, rec.committedIDs(), "OnCommitted fired per-record in order after commit")
	assert.Equal(t, 3, inboxCount(t, pool, "grp-batch-clean"))
}

// TestBatchHandler_PoisonFallsBackAndReportsProcessed — batch [good, poison,
// good2]; the batch tx fails on poison; fallback applies good, then poison's
// per-record processing errors → return (processed=1, err!=nil). good2 (after
// poison) must NOT be applied, and good must be applied exactly once.
func TestBatchHandler_PoisonFallsBackAndReportsProcessed(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	rec := &recorder{}
	h := consume.New(pg.WrapPool(pool), "grp-batch-poison").BatchHandler(
		nil, // fallback defaults to processRecord
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			if evt.GetOrderId() == "poison" {
				return errors.New("poison pill")
			}
			rec.record(evt.GetOrderId())
			return nil
		}),
	)

	records := []kafka.Record{
		orderRecordWithID(t, "good", "m-good", 0, 1),
		orderRecordWithID(t, "poison", "m-poison", 0, 2),
		orderRecordWithID(t, "good2", "m-good2", 0, 3),
	}

	processed, err := h(ctx, records)
	require.Error(t, err, "poison must propagate so kafka seeks back")
	assert.Equal(t, 1, processed, "processed counts records applied before the failure (index into original slice)")

	// "good" may be invoked during the doomed batch tx (which rolls back) AND
	// again in the fallback — the in-memory recorder is not transactional, so
	// it can see both. The DURABLE guarantee is what matters: exactly one inbox
	// row (good's) persists; poison rolled back, good2 (after poison in offset
	// order) was never reached.
	for _, id := range rec.appliedIDs() {
		assert.NotEqual(t, "good2", id, "good2 is after poison in offset order; must never apply")
	}
	assert.Equal(t, 1, inboxCount(t, pool, "grp-batch-poison"),
		"strict all-or-nothing: only good's row persists durably (exactly once)")
}

// TestBatchHandler_DedupSkipsSeen — re-delivering an already-applied batch
// applies nothing (ProcessBatchOnce returns 0 newly-applied) and OnCommitted
// does not re-fire for seen ids; the function still returns len(records).
func TestBatchHandler_DedupSkipsSeen(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	rec := &recorder{}
	h := consume.New(pg.WrapPool(pool), "grp-batch-dedup").BatchHandler(
		nil, // fallback defaults to processRecord
		consume.Typed(
			"orders.OrderCreated.v1",
			func(_ context.Context, evt *ordersv1.OrderCreated) error {
				rec.record(evt.GetOrderId())
				return nil
			},
			consume.OnCommitted(func(_ context.Context, evt *ordersv1.OrderCreated) {
				rec.commit(evt.GetOrderId())
			}),
		),
	)

	records := []kafka.Record{
		orderRecordWithID(t, "x", "m-x", 0, 1),
		orderRecordWithID(t, "y", "m-y", 0, 2),
	}

	processed, err := h(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, 2, processed)
	assert.Equal(t, []string{"x", "y"}, rec.appliedIDs())
	assert.Equal(t, []string{"x", "y"}, rec.committedIDs())

	// Re-deliver the same batch: all ids already seen → nothing re-applied,
	// OnCommitted does NOT re-fire, but the batch is fully acked.
	processed, err = h(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, 2, processed, "re-delivered batch is fully acked")
	assert.Equal(t, []string{"x", "y"}, rec.appliedIDs(), "no double-apply on redelivery")
	assert.Equal(t, []string{"x", "y"}, rec.committedIDs(), "OnCommitted does not re-fire for seen ids")
	assert.Equal(t, 2, inboxCount(t, pool, "grp-batch-dedup"))
}

// TestBatchHandler_SkipsUnknownEventTypes — records with an unregistered
// event-type header are acked-skipped, not failed; a mixed batch [known,
// unknown, known] applies both known ones and returns len(records).
func TestBatchHandler_SkipsUnknownEventTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	rec := &recorder{}
	h := consume.New(pg.WrapPool(pool), "grp-batch-unknown").BatchHandler(
		nil, // fallback defaults to processRecord
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			rec.record(evt.GetOrderId())
			return nil
		}),
	)

	unknown := orderRecordWithID(t, "u", "m-u", 0, 2)
	unknown.Headers["event-type"] = "orders.Unknown.v9"

	records := []kafka.Record{
		orderRecordWithID(t, "k1", "m-k1", 0, 1),
		unknown,
		orderRecordWithID(t, "k2", "m-k2", 0, 3),
	}

	processed, err := h(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, len(records), processed, "unknown event types are acked-skipped, whole batch acked")
	assert.Equal(t, []string{"k1", "k2"}, rec.appliedIDs(), "both known records applied; unknown skipped")
	assert.Equal(t, 2, inboxCount(t, pool, "grp-batch-unknown"))
}
