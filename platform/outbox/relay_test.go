package outbox_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/pg"
)

type fakePublisher struct {
	mu       sync.Mutex
	received []outbox.Message
	failNext bool
}

func (f *fakePublisher) Publish(_ context.Context, msg outbox.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return context.DeadlineExceeded
	}
	f.received = append(f.received, msg)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func TestRelay_PublishesUnpublishedAndMarksThem(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	for i := 0; i < 3; i++ {
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: uuid.New(), AggregateType: "order", AggregateID: "x",
				EventType: "OrderCreated", Payload: []byte(`{}`),
			})
		}))
	}

	pub := &fakePublisher{}
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	n, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, 3, pub.count())

	// All marked published → a second batch processes nothing.
	n2, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n2)
}

func TestRelay_PublishFailureLeavesRowUnpublished(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID: uuid.New(), AggregateType: "order", AggregateID: "x",
			EventType: "OrderCreated", Payload: []byte(`{}`),
		})
	}))

	pub := &fakePublisher{failNext: true}
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{BatchSize: 10})

	_, err := relay.ProcessBatch(ctx)
	require.Error(t, err)

	var unpublished int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&unpublished))
	require.Equal(t, 1, unpublished, "failed publish must not mark row published")
}
