package outbox_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// messages returns a copy of the received slice (thread-safe).
func (f *fakePublisher) messages() []outbox.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]outbox.Message, len(f.received))
	copy(out, f.received)
	return out
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

// alwaysFailPublisher always returns an error from Publish.
type alwaysFailPublisher struct {
	calls atomic.Int64
}

func (a *alwaysFailPublisher) Publish(_ context.Context, _ outbox.Message) error {
	a.calls.Add(1)
	return context.DeadlineExceeded
}

// TestRelay_RunCallsOnErrorAndKeepsPolling verifies that Run calls OnError on
// batch failures and keeps polling until the context is cancelled.
func TestRelay_RunCallsOnErrorAndKeepsPolling(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	// Enqueue one message so ProcessBatch actually calls Publish (and fails).
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID: uuid.New(), AggregateType: "order", AggregateID: "x",
			EventType: "OrderCreated", Payload: []byte(`{}`),
		})
	}))

	pub := &alwaysFailPublisher{}
	var onErrorCalls atomic.Int64

	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{
		BatchSize:    10,
		PollInterval: 20 * time.Millisecond,
	})
	relay.SetOnError(func(_ error) {
		onErrorCalls.Add(1)
	})

	runCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- relay.Run(runCtx)
	}()

	err := <-done
	// Run should return promptly after context cancellation.
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// OnError must have been called at least once.
	require.Greater(t, onErrorCalls.Load(), int64(0), "OnError must be called at least once")
}

// TestRelay_ConcurrentProcessBatchNoDoublePublish verifies that for-update-skip-locked
// prevents two concurrent ProcessBatch calls from publishing the same message twice.
// N=20 messages enqueued; two relays run ProcessBatch concurrently; the union of
// both publishers' received messages must contain exactly 20 unique IDs.
func TestRelay_ConcurrentProcessBatchNoDoublePublish(t *testing.T) {
	const N = 20
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	ids := make([]uuid.UUID, N)
	for i := 0; i < N; i++ {
		ids[i] = uuid.New()
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID:            ids[i],
				AggregateType: "order",
				AggregateID:   "x",
				EventType:     "OrderCreated",
				Payload:       []byte(`{}`),
			})
		}))
	}

	pub1 := &fakePublisher{}
	pub2 := &fakePublisher{}

	relay1 := outbox.NewRelay(pool, pub1, outbox.RelayConfig{BatchSize: int32(N)})
	relay2 := outbox.NewRelay(pool, pub2, outbox.RelayConfig{BatchSize: int32(N)})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = relay1.ProcessBatch(ctx) }()
	go func() { defer wg.Done(); _, _ = relay2.ProcessBatch(ctx) }()
	wg.Wait()

	// Collect all received message IDs from both publishers.
	seen := make(map[uuid.UUID]int)
	for _, msg := range pub1.messages() {
		seen[msg.ID]++
	}
	for _, msg := range pub2.messages() {
		seen[msg.ID]++
	}

	totalPublished := pub1.count() + pub2.count()
	require.Equal(t, N, totalPublished, "total published across both relays must be exactly %d", N)

	for _, id := range ids {
		count := seen[id]
		require.Equal(t, 1, count, "message %s must appear exactly once across both publishers", id)
	}
}

// TestRelay_RepublishesAfterTransientFailure verifies at-least-once semantics:
// a message that survives a failed ProcessBatch is published on the next attempt.
func TestRelay_RepublishesAfterTransientFailure(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID:            id,
			AggregateType: "order",
			AggregateID:   "x",
			EventType:     "OrderCreated",
			Payload:       []byte(`{}`),
		})
	}))

	// First ProcessBatch: publisher fails → row stays unpublished.
	failPub := &fakePublisher{failNext: true}
	relay1 := outbox.NewRelay(pool, failPub, outbox.RelayConfig{BatchSize: 10})
	_, err := relay1.ProcessBatch(ctx)
	require.Error(t, err, "first ProcessBatch should fail")
	require.Equal(t, 0, failPub.count(), "no message should be published on failure")

	// Verify the row is still unpublished.
	var unpublished int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null and id = $1`, id).Scan(&unpublished))
	require.Equal(t, 1, unpublished, "row must remain unpublished after failed batch")

	// Second ProcessBatch: publisher succeeds → message published.
	succeedPub := &fakePublisher{}
	relay2 := outbox.NewRelay(pool, succeedPub, outbox.RelayConfig{BatchSize: 10})
	n, err := relay2.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "message must be published on retry")
	require.Equal(t, 1, succeedPub.count())
	require.Equal(t, id, succeedPub.messages()[0].ID, "republished message must have the original ID")
}
