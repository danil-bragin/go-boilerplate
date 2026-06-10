package consume_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// newPool spins up a Postgres container with the inbox schema.
func newPool(t *testing.T) *pg.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	_, err = pool.Writer().Exec(ctx, `
		create table inbox (
			consumer     text not null,
			message_id   text not null,
			processed_at timestamptz not null default now(),
			primary key (consumer, message_id)
		)`)
	require.NoError(t, err)
	return pool
}

func orderCreatedRecord(t *testing.T, headers map[string]string, partition int32, offset int64) kafka.Record {
	t.Helper()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: "o1", CustomerId: "c1", AmountCents: 100, Currency: "USD",
	})
	require.NoError(t, err)
	return kafka.Record{
		Topic:     "orders.events",
		Key:       []byte("o1"),
		Value:     payload,
		Headers:   headers,
		Partition: partition,
		Offset:    offset,
	}
}

func TestTyped_DispatchDecodeAndDedup(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	var calls atomic.Int64
	var got *ordersv1.OrderCreated
	h := consume.New(pool, "grp", consume.WithLogger(slog.Default())).Handler(
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			calls.Add(1)
			got = evt
			return nil
		}),
	)

	rec := orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m1",
	}, 0, 1)

	require.NoError(t, h(ctx, rec))
	require.EqualValues(t, 1, calls.Load())
	require.NotNil(t, got)
	assert.Equal(t, "o1", got.GetOrderId())
	assert.Equal(t, int64(100), got.GetAmountCents())

	// Same message-id → inbox dedup, handler NOT called again.
	require.NoError(t, h(ctx, rec))
	assert.EqualValues(t, 1, calls.Load(), "duplicate message-id must be deduplicated")
}

func TestTyped_UnknownEventTypeSkipped(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	var calls atomic.Int64
	h := consume.New(pool, "grp").Handler(
		consume.Typed("orders.OrderCreated.v1", func(context.Context, *ordersv1.OrderCreated) error {
			calls.Add(1)
			return nil
		}),
	)

	// Unknown event type → skip silently (forward compatible), no error.
	rec := orderCreatedRecord(t, map[string]string{"event-type": "orders.Unknown.v9", "message-id": "m2"}, 0, 2)
	require.NoError(t, h(ctx, rec))

	// Missing event-type header → also skipped (producers always set it).
	rec = orderCreatedRecord(t, map[string]string{"message-id": "m3"}, 0, 3)
	require.NoError(t, h(ctx, rec))

	assert.EqualValues(t, 0, calls.Load())
}

func TestTyped_MessageIDFallbackTopicPartitionOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	var calls atomic.Int64
	h := consume.New(pool, "grp").Handler(
		consume.Typed("orders.OrderCreated.v1", func(context.Context, *ordersv1.OrderCreated) error {
			calls.Add(1)
			return nil
		}),
	)

	hdrs := map[string]string{"event-type": "orders.OrderCreated.v1"} // no message-id

	require.NoError(t, h(ctx, orderCreatedRecord(t, hdrs, 1, 42)))
	require.NoError(t, h(ctx, orderCreatedRecord(t, hdrs, 1, 42))) // same position → dup
	require.NoError(t, h(ctx, orderCreatedRecord(t, hdrs, 1, 43))) // next offset → new

	assert.EqualValues(t, 2, calls.Load(), "fallback id must be topic:partition:offset")

	var id string
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select message_id from inbox where message_id like 'orders.events%' order by message_id limit 1`).Scan(&id))
	assert.Equal(t, "orders.events:1:42", id)
}

func TestTyped_HandlerErrorPropagatesAndRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	fail := true
	var calls atomic.Int64
	h := consume.New(pool, "grp").Handler(
		consume.Typed("orders.OrderCreated.v1", func(context.Context, *ordersv1.OrderCreated) error {
			calls.Add(1)
			if fail {
				return errors.New("transient")
			}
			return nil
		}),
	)

	rec := orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m-err",
	}, 0, 7)

	require.Error(t, h(ctx, rec), "handler error must propagate (no commit, redelivery)")

	// Inbox row must have rolled back → retry processes the message.
	fail = false
	require.NoError(t, h(ctx, rec))
	assert.EqualValues(t, 2, calls.Load())
}

// stubDeserializer asserts the consumer uses the injected deserializer instead
// of raw proto.Unmarshal.
type stubDeserializer struct{ used atomic.Bool }

func (s *stubDeserializer) Decode(data []byte, into proto.Message) error {
	s.used.Store(true)
	return proto.Unmarshal(data, into)
}

func TestTyped_SerdeDecodePath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	dec := &stubDeserializer{}
	var calls atomic.Int64
	h := consume.New(pool, "grp", consume.WithSerde(dec)).Handler(
		consume.Typed("orders.OrderCreated.v1", func(context.Context, *ordersv1.OrderCreated) error {
			calls.Add(1)
			return nil
		}),
	)

	rec := orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m-serde",
	}, 0, 9)
	require.NoError(t, h(ctx, rec))
	assert.EqualValues(t, 1, calls.Load())
	assert.True(t, dec.used.Load(), "injected deserializer must be used")
}

func TestTyped_OnCommittedRunsAfterTx(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	var committed atomic.Int64
	h := consume.New(pool, "grp").Handler(
		consume.Typed("orders.OrderCreated.v1",
			func(ctx context.Context, _ *ordersv1.OrderCreated) error {
				// Inside the inbox tx the row is not yet visible to other conns.
				var n int
				require.NoError(t, pool.Reader().QueryRow(ctx,
					`select count(*) from inbox where message_id = 'm-pc'`).Scan(&n))
				require.Zero(t, n, "callback ordering: tx must not be committed yet")
				return nil
			},
			func(_ context.Context, evt *ordersv1.OrderCreated) {
				committed.Add(1)
				// After commit the inbox row IS visible.
				var n int
				require.NoError(t, pool.Reader().QueryRow(ctx,
					`select count(*) from inbox where message_id = 'm-pc'`).Scan(&n))
				require.Equal(t, 1, n, "onCommitted must run after the inbox tx commits")
				require.Equal(t, "o1", evt.GetOrderId())
			},
		),
	)

	rec := orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m-pc",
	}, 0, 11)
	require.NoError(t, h(ctx, rec))
	assert.EqualValues(t, 1, committed.Load())
}

func TestTyped_CorrelationCausationInContext(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	t.Parallel()
	ctx := context.Background()
	pool := newPool(t)

	var gotCorr, gotParent string
	h := consume.New(pool, "grp").Handler(
		consume.Typed("orders.OrderCreated.v1", func(ctx context.Context, _ *ordersv1.OrderCreated) error {
			gotCorr = msgctx.CorrelationID(ctx)
			gotParent = msgctx.ParentMessageID(ctx)
			return nil
		}),
	)

	// Record WITH a correlation-id header: both ids visible in ctx.
	rec := orderCreatedRecord(t, map[string]string{
		"event-type":     "orders.OrderCreated.v1",
		"message-id":     "m-corr-1",
		"correlation-id": "root-cmd-id",
	}, 0, 21)
	require.NoError(t, h(ctx, rec))
	assert.Equal(t, "root-cmd-id", gotCorr, "correlation-id header must propagate to ctx")
	assert.Equal(t, "m-corr-1", gotParent, "current message-id must become the causation parent")

	// Record WITHOUT a correlation-id header: this message starts the chain,
	// so correlation falls back to its own message id.
	rec = orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m-corr-2",
	}, 0, 22)
	require.NoError(t, h(ctx, rec))
	assert.Equal(t, "m-corr-2", gotCorr, "missing correlation-id must default to the message id")
	assert.Equal(t, "m-corr-2", gotParent)
}

// TestTyped_WithoutInbox_FastLane covers the test-only escape hatch: the full
// typed pipeline (header dispatch, decode, ctx propagation, onCommitted) runs
// with NO database — pool may be nil. Without the inbox there is no dedup:
// every delivery runs the handler. This is what example-service fast-lane
// tests build on (fakes.Broker + WithoutInbox).
func TestTyped_WithoutInbox_FastLane(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var calls, committed atomic.Int32
	var gotCorr string
	h := consume.New(nil, "grp", consume.WithoutInbox()).Handler(
		consume.Typed("orders.OrderCreated.v1",
			func(ctx context.Context, evt *ordersv1.OrderCreated) error {
				calls.Add(1)
				gotCorr = msgctx.CorrelationID(ctx)
				assert.Equal(t, "o1", evt.GetOrderId(), "payload must be decoded")
				return nil
			},
			func(context.Context, *ordersv1.OrderCreated) { committed.Add(1) },
		),
	)

	rec := orderCreatedRecord(t, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": "m-ni-1",
	}, 0, 1)
	require.NoError(t, h(ctx, rec))
	require.NoError(t, h(ctx, rec), "no inbox → no dedup: redelivery runs the handler again")
	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, int32(2), committed.Load(), "onCommitted runs after each successful handle")
	assert.Equal(t, "m-ni-1", gotCorr, "msgctx propagation works without the inbox")

	// Unknown event types are still skipped, and handler errors still propagate.
	require.NoError(t, h(ctx, orderCreatedRecord(t, map[string]string{"event-type": "nope.v9"}, 0, 2)))
	assert.Equal(t, int32(2), calls.Load())

	boom := errors.New("boom")
	hErr := consume.New(nil, "grp", consume.WithoutInbox()).Handler(
		consume.Typed("orders.OrderCreated.v1", func(context.Context, *ordersv1.OrderCreated) error {
			return boom
		}),
	)
	require.ErrorIs(t, hErr(ctx, rec), boom)
}
