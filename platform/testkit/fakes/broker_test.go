package fakes_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// Interface conformance: the broker must be a drop-in outbox publisher so
// service wiring under test swaps outboxkafka.KafkaPublisher for it.
var (
	_ outbox.Publisher      = (*fakes.Broker)(nil)
	_ outbox.BatchPublisher = (*fakes.Broker)(nil)
)

// TestMain verifies the broker spawns no goroutines — it must stay fully
// synchronous so fast-lane tests need no sleeps and leak nothing.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestBroker_ProduceAssignsPositionPerTopic(t *testing.T) {
	b := fakes.NewBroker()
	ctx := context.Background()

	require.NoError(t, b.Produce(ctx, kafka.Record{Topic: "orders.events", Value: []byte("a")}))
	require.NoError(t, b.Produce(ctx, kafka.Record{Topic: "orders.events", Value: []byte("b")}))
	require.NoError(t, b.Produce(ctx, kafka.Record{Topic: "payments.events", Value: []byte("c")}))

	orders := b.Records("orders.events")
	require.Len(t, orders, 2)
	assert.Equal(t, int32(0), orders[0].Partition)
	assert.Equal(t, int64(0), orders[0].Offset)
	assert.Equal(t, int64(1), orders[1].Offset)

	payments := b.Records("payments.events")
	require.Len(t, payments, 1)
	assert.Equal(t, int64(0), payments[0].Offset, "offsets are per topic")
}

func TestBroker_DispatchesSynchronouslyWithHeaders(t *testing.T) {
	b := fakes.NewBroker()
	var got []kafka.Record
	b.Subscribe("orders.events", func(_ context.Context, r kafka.Record) error {
		got = append(got, r)
		return nil
	})

	rec := kafka.Record{
		Topic:   "orders.events",
		Key:     []byte("agg-1"),
		Value:   []byte("payload"),
		Headers: map[string]string{"event-type": "orders.OrderCreated.v1", "correlation-id": "corr-1"},
	}
	require.NoError(t, b.Produce(context.Background(), rec))

	require.Len(t, got, 1, "dispatch is synchronous — no sleeps needed")
	assert.Equal(t, "orders.OrderCreated.v1", got[0].Headers["event-type"])
	assert.Equal(t, "corr-1", got[0].Headers["correlation-id"])
	assert.Equal(t, []byte("agg-1"), got[0].Key)
	assert.Equal(t, int32(0), got[0].Partition)
	assert.Equal(t, int64(0), got[0].Offset, "subscriber sees the assigned position")
}

func TestBroker_TopicIsolation(t *testing.T) {
	b := fakes.NewBroker()
	calls := 0
	b.Subscribe("payments.events", func(context.Context, kafka.Record) error {
		calls++
		return nil
	})

	require.NoError(t, b.Produce(context.Background(), kafka.Record{Topic: "orders.events"}))
	assert.Zero(t, calls, "subscriber on another topic must not fire")
}

func TestBroker_HandlerErrorPropagates(t *testing.T) {
	b := fakes.NewBroker()
	boom := errors.New("boom")
	b.Subscribe("t", func(context.Context, kafka.Record) error { return boom })
	b.Subscribe("t", func(context.Context, kafka.Record) error { return nil })

	err := b.Produce(context.Background(), kafka.Record{Topic: "t"})
	require.ErrorIs(t, err, boom)

	// The record is still appended (broker accepted it; the consumer failed).
	assert.Len(t, b.Records("t"), 1)
}

func TestBroker_PublishMapsOutboxMessageLikeKafkaPublisher(t *testing.T) {
	b := fakes.NewBroker()
	id := uuid.New()
	msg := outbox.Message{
		ID:            id,
		Topic:         "orders.events",
		AggregateType: "order",
		AggregateID:   "order-1",
		EventType:     "orders.OrderCreated.v1",
		Payload:       []byte("proto-bytes"),
		Headers:       []byte(`{"correlation-id":"corr-9"}`),
	}
	require.NoError(t, b.Publish(context.Background(), msg))

	recs := b.Records("orders.events")
	require.Len(t, recs, 1)
	r := recs[0]
	assert.Equal(t, []byte("order-1"), r.Key, "keyed by aggregate id — ordering parity with outboxkafka")
	assert.Equal(t, []byte("proto-bytes"), r.Value)
	assert.Equal(t, "orders.OrderCreated.v1", r.Headers["event-type"])
	assert.Equal(t, id.String(), r.Headers["message-id"])
	assert.Equal(t, "order", r.Headers["aggregate-type"])
	assert.Equal(t, "corr-9", r.Headers["correlation-id"], "custom JSON headers merged")
}

func TestBroker_PublishTopicFallbackAndMalformedHeaders(t *testing.T) {
	b := fakes.NewBroker()
	msg := outbox.Message{
		ID:            uuid.New(),
		AggregateType: "order", // no Topic → AggregateType fallback (legacy rows)
		AggregateID:   "order-2",
		EventType:     "orders.OrderCreated.v1",
		Headers:       []byte(`{not json`),
	}
	require.NoError(t, b.Publish(context.Background(), msg))

	recs := b.Records("order")
	require.Len(t, recs, 1)
	assert.Equal(t, "orders.OrderCreated.v1", recs[0].Headers["event-type"],
		"standard headers survive malformed custom headers (head-of-line-block parity)")
}

func TestBroker_PublishBatchDeliversAll(t *testing.T) {
	b := fakes.NewBroker()
	var seen []string
	b.Subscribe("t", func(_ context.Context, r kafka.Record) error {
		seen = append(seen, r.Headers["message-id"])
		return nil
	})

	msgs := []outbox.Message{
		{ID: uuid.New(), Topic: "t", EventType: "e.v1"},
		{ID: uuid.New(), Topic: "t", EventType: "e.v1"},
	}
	require.NoError(t, b.PublishBatch(context.Background(), msgs))
	assert.Equal(t, []string{msgs[0].ID.String(), msgs[1].ID.String()}, seen)
}

func TestBroker_RecordsReturnsCopy(t *testing.T) {
	b := fakes.NewBroker()
	require.NoError(t, b.Produce(context.Background(), kafka.Record{Topic: "t", Value: []byte("v")}))

	got := b.Records("t")
	got[0].Value = []byte("mutated")
	assert.Equal(t, []byte("v"), b.Records("t")[0].Value)
	assert.Empty(t, b.Records("unknown"))
}

func TestBroker_HandlerMayProduceFollowUps(t *testing.T) {
	// A consumer that emits a follow-up event re-enters the broker; this
	// must not deadlock (choreography chains in one synchronous call).
	b := fakes.NewBroker()
	var notified []string
	b.Subscribe("orders.events", func(ctx context.Context, r kafka.Record) error {
		return b.Produce(ctx, kafka.Record{Topic: "payments.events", Value: r.Value})
	})
	b.Subscribe("payments.events", func(_ context.Context, r kafka.Record) error {
		notified = append(notified, string(r.Value))
		return nil
	})

	require.NoError(t, b.Produce(context.Background(), kafka.Record{Topic: "orders.events", Value: []byte("o1")}))
	assert.Equal(t, []string{"o1"}, notified)
}

func TestBroker_ConcurrentProduceIsSafe(t *testing.T) {
	b := fakes.NewBroker()
	b.Subscribe("t", func(context.Context, kafka.Record) error { return nil })

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				_ = b.Produce(context.Background(), kafka.Record{Topic: "t"})
			}
		}()
	}
	wg.Wait()

	recs := b.Records("t")
	require.Len(t, recs, 200)
	offsets := make(map[int64]bool, len(recs))
	for _, r := range recs {
		offsets[r.Offset] = true
	}
	assert.Len(t, offsets, 200, "offsets are unique and monotonically assigned")
}
