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
