package outboxkafka_test

import (
	"context"
	"embed"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/kafka/kafkatest"
	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/outboxkafka"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"
)

//go:embed testdata/migrations/*.sql
var testMigrations embed.FS

func TestKafkaPublisher_DrainsOutboxToKafka(t *testing.T) {
	ctx := context.Background()

	// 1. Start Postgres and Redpanda containers.
	dsn := pgtest.NewDSN(t)
	broker, _ := kafkatest.NewRedpanda(t)

	// 2. Migrate the outbox schema using the local copy.
	require.NoError(t, pg.Migrate(ctx, dsn, testMigrations, "testdata/migrations"))

	// 3. Build pool, Kafka client, producer, and ensure the topic exists.
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "t",
	})
	require.NoError(t, err)
	defer cl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, cl, 1, 1, "order"))

	producer := kafka.NewProducer(cl)
	defer func() { _ = producer.Close(ctx) }()

	// 4. Build KafkaPublisher and relay.
	pub := outboxkafka.New(producer)
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	// 5. Enqueue 3 messages with distinct aggregate IDs.
	repo := outbox.NewRepository(pool)
	type enqueued struct {
		id          uuid.UUID
		aggregateID string
		payload     []byte
	}
	messages := []enqueued{
		{id: uuid.New(), aggregateID: "order-1", payload: []byte(`{"order":"1"}`)},
		{id: uuid.New(), aggregateID: "order-2", payload: []byte(`{"order":"2"}`)},
		{id: uuid.New(), aggregateID: "order-3", payload: []byte(`{"order":"3"}`)},
	}
	for _, m := range messages {
		m := m
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID:            m.id,
				AggregateType: "order",
				AggregateID:   m.aggregateID,
				EventType:     "OrderCreated",
				Payload:       m.payload,
			})
		}))
	}

	// 6. Process the batch — expect exactly 3 published.
	n, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// 7. Consume from the "order" topic and collect 3 records (10 s timeout).
	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "t-consumer",
		GroupID:  "t-grp",
	}, "order")
	require.NoError(t, err)
	defer consumer.Close()

	var (
		mu       sync.Mutex
		received []kafka.Record
		done     = make(chan struct{})
	)

	consumeCtx, cancelConsume := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConsume()

	go func() {
		_ = consumer.Run(consumeCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			received = append(received, r)
			count := len(received)
			mu.Unlock()
			if count >= 3 {
				cancelConsume()
			}
			return nil
		})
		close(done)
	}()

	<-done

	mu.Lock()
	got := make([]kafka.Record, len(received))
	copy(got, received)
	mu.Unlock()

	require.Len(t, got, 3, "expected 3 records from Kafka")

	// Build expected sets for assertion (order may differ across partitions).
	wantKeys := make(map[string]struct{}, 3)
	wantPayloads := make(map[string]struct{}, 3)
	for _, m := range messages {
		wantKeys[m.aggregateID] = struct{}{}
		wantPayloads[string(m.payload)] = struct{}{}
	}

	for _, r := range got {
		assert.Contains(t, wantKeys, string(r.Key), "unexpected key %s", r.Key)
		assert.Contains(t, wantPayloads, string(r.Value), "unexpected payload %s", r.Value)
		assert.Equal(t, "OrderCreated", r.Headers["event-type"], "event-type header must be OrderCreated")
		assert.NotEmpty(t, r.Headers["message-id"], "message-id header must be set")
		assert.Equal(t, "order", r.Headers["aggregate-type"], "aggregate-type header must be order")
	}

	// 8. All outbox rows must be marked published.
	var unpublished int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&unpublished))
	require.Equal(t, 0, unpublished, "all outbox rows must be marked published")
}

// TestKafkaPublisher_BrokerDownLeavesRowsUnpublished verifies that when the
// Kafka broker is unreachable the relay's ProcessBatch rolls back its
// transaction and leaves outbox rows with published_at IS NULL.
//
// This exercises the at-least-once guarantee from the other direction: a
// produce failure must NEVER silently mark rows as published. The row is
// preserved for retry on the next poll cycle.
//
// Implementation note: we point the producer at a dead address
// (127.0.0.1:1) and configure franz-go to give up quickly via
// RequestRetries(0) and a short ProduceRequestTimeout so the test stays
// under ~5 s total.
func TestKafkaPublisher_BrokerDownLeavesRowsUnpublished(t *testing.T) {
	ctx := context.Background()

	// 1. Start Postgres only (no Redpanda).
	dsn := pgtest.NewDSN(t)
	require.NoError(t, pg.Migrate(ctx, dsn, testMigrations, "testdata/migrations"))

	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	// 2. Build a kafka client pointed at an unreachable address.
	// RequestRetries(0) disables internal request retries so the produce
	// fails fast; ProduceRequestTimeout bounds how long franz-go waits for
	// a broker response before giving up.
	deadCl, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.ClientID("broker-down-test"),
		kgo.RequestRetries(0),
		kgo.ProduceRequestTimeout(500*time.Millisecond),
		kgo.RecordRetries(0),
		kgo.RetryTimeout(500*time.Millisecond),
	)
	require.NoError(t, err)
	defer deadCl.Close()

	producer := kafka.NewProducer(deadCl)

	// 3. Build KafkaPublisher and relay.
	pub := outboxkafka.New(producer)
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	// 4. Enqueue 1 message.
	repo := outbox.NewRepository(pool)
	msgID := uuid.New()
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID:            msgID,
			AggregateType: "order",
			AggregateID:   "order-99",
			EventType:     "OrderCreated",
			Payload:       []byte(`{"order":"99"}`),
		})
	}))

	// 5. ProcessBatch with a short deadline — the produce must fail and the
	// transaction must roll back before the deadline.
	batchCtx, cancelBatch := context.WithTimeout(ctx, 3*time.Second)
	defer cancelBatch()

	_, batchErr := relay.ProcessBatch(batchCtx)
	assert.Error(t, batchErr, "ProcessBatch must return an error when broker is unreachable")

	// 6. The row must still be unpublished (transaction rolled back).
	var unpublished int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&unpublished))
	assert.Equal(t, 1, unpublished,
		"outbox row must remain unpublished when broker is down (no data loss)")
}
