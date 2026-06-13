package consume_test

import (
	"context"
	"sync"
	"testing"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestBatchHandler_CrashBeforeOffsetCommit_NoDuplicateOnRestart proves the
// crash-safety contract Task 7 part 3 requires: if the consumer dies mid-poll —
// after the side effects ran in the batch tx but BEFORE the per-poll Kafka
// offset is committed — the whole poll is re-delivered on restart and the
// projection applies it EXACTLY ONCE. The inbox all-or-nothing tx makes this
// deterministic: a crash before the offset commit means the tx never committed
// (nothing persisted) OR it committed (inbox rows present, redelivery deduped).
// Either way, no duplicate side effects.
//
// We model the crash by cancelling the batch ctx mid-tx on the first delivery:
// pg.RunInTx sees the cancelled ctx and rolls back, so the inbox tx persists
// NOTHING and the BatchHandler's fallback (also ctx-cancelled) makes no
// progress — exactly the "offset not advanced, nothing committed" state of a
// process that died mid-poll. Kafka then re-delivers the identical poll; the
// restarted handler (fresh ctx) applies it once.
func TestBatchHandler_CrashBeforeOffsetCommit_NoDuplicateOnRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	const group = "grp-batch-crash"

	var mu sync.Mutex
	applied := map[string]int{} // order id -> times the side effect ran durably

	// crashCtx is cancelled the instant the side effect for the "crash trigger"
	// record runs on the FIRST delivery, simulating a process death mid-poll
	// (before the per-poll offset commit). pg.RunInTx observes the cancellation
	// and rolls the whole batch back.
	crashCtx, crash := context.WithCancel(ctx)
	first := true

	makeHandler := func() kafka.BatchHandlerFunc {
		return consume.New(pg.WrapPool(pool), group).BatchHandler(
			nil,
			consume.Typed("orders.OrderCreated.v1", func(hctx context.Context, evt *ordersv1.OrderCreated) error {
				// Record the side effect only when its tx actually commits: we
				// approximate that by recording at call time but asserting on the
				// DURABLE inbox count below, which is the real exactly-once proof.
				mu.Lock()
				applied[evt.GetOrderId()]++
				mu.Unlock()
				if first && evt.GetOrderId() == "b" {
					// Die mid-poll: cancel so RunInTx rolls back this batch.
					crash()
					return hctx.Err()
				}
				return nil
			}),
		)
	}

	records := []kafka.Record{
		orderRecordWithID(t, "a", "m-a", 0, 1),
		orderRecordWithID(t, "b", "m-b", 0, 2),
		orderRecordWithID(t, "c", "m-c", 0, 3),
	}

	// --- Delivery 1: crash mid-poll. Nothing must persist. ---
	h1 := makeHandler()
	_, err := h1(crashCtx, records)
	require.Error(t, err, "a ctx-cancelled poll must surface an error so kafka seeks back (offset NOT advanced)")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, inboxCount(t, pool, group),
		"crash before the offset commit must leave NO durable inbox rows (batch tx rolled back)")

	// --- Restart: fresh ctx, identical poll re-delivered. ---
	first = false
	h2 := makeHandler()
	processed, err := h2(ctx, records)
	require.NoError(t, err, "restart re-applies the whole poll cleanly")
	assert.Equal(t, len(records), processed, "whole poll acked on restart")
	assert.Equal(t, 3, inboxCount(t, pool, group),
		"exactly-once: one inbox row per record after restart, no duplicates")

	// --- Re-deliver once more (post-commit redelivery storm): all deduped. ---
	processed, err = h2(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, len(records), processed, "redelivered poll is fully acked")
	assert.Equal(t, 3, inboxCount(t, pool, group),
		"no double-apply: inbox dedup absorbs the redelivery")
}
