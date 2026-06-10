package outbox

// Internal (white-box) test for the drain-loop leadership re-check: it needs
// access to the unexported drain/ensureLeader methods to deterministically
// kill the leader's advisory-lock session between two batches of one drain.

import (
	"context"
	"sync"
	"testing"

	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// killingBatchPublisher records published batches and invokes kill exactly
// once, right after the first successful batch — simulating the leader losing
// its advisory-lock session mid-drain.
type killingBatchPublisher struct {
	mu       sync.Mutex
	received []Message
	kill     func()
	killed   bool
}

func (p *killingBatchPublisher) Publish(_ context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.received = append(p.received, msg)
	return nil
}

func (p *killingBatchPublisher) PublishBatch(_ context.Context, msgs []Message) error {
	p.mu.Lock()
	p.received = append(p.received, msgs...)
	shouldKill := !p.killed
	p.killed = true
	p.mu.Unlock()
	if shouldKill && p.kill != nil {
		p.kill()
	}
	return nil
}

func (p *killingBatchPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.received)
}

// TestDrain_StopsWhenLeadershipLostMidDrain verifies that a leader whose
// advisory-lock session dies between two batches of one drain STOPS draining
// after the batch in flight, instead of publishing the whole backlog while a
// standby may already have taken over (unbounded dual-publish ordering
// violation). Leadership is re-verified between drain iterations.
func TestDrain_StopsWhenLeadershipLostMidDrain(t *testing.T) {
	pool := newMetricsPool(t) // postgres container + outbox migrations
	ctx := context.Background()

	const batchSize = 5
	repo := NewRepository(pool)
	for i := 0; i < 3*batchSize; i++ {
		require.NoError(t, pg.RunInTx(ctx, pool, func(txCtx context.Context) error {
			return repo.Enqueue(txCtx, Message{
				ID: uuid.New(), AggregateType: "order", AggregateID: "agg",
				EventType: "OrderCreated", Payload: []byte(`{}`),
			})
		}))
	}

	pub := &killingBatchPublisher{}
	r := NewRelay(pool, pub, RelayConfig{BatchSize: batchSize}, WithSingleActive(pool.Writer()))

	// Become leader, then arrange for the lock session to be terminated right
	// after the first batch is published.
	require.True(t, r.ensureLeader(ctx), "relay must win the advisory lock")
	t.Cleanup(func() { r.dropLeadership(ctx, true) })

	pub.kill = func() {
		_, err := pool.Writer().Exec(ctx, `
			select pg_terminate_backend(l.pid)
			from pg_locks l
			where l.locktype = 'advisory' and l.granted
			  and l.pid <> pg_backend_pid()`)
		require.NoError(t, err)
	}

	require.NoError(t, r.drain(ctx))

	// Only the in-flight first batch may have been published…
	require.Equal(t, batchSize, pub.count(),
		"drain must stop after the batch during which leadership was lost")

	// …and the rest of the backlog stays for the new leader.
	var unpublished int
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&unpublished))
	require.Equal(t, 2*batchSize, unpublished,
		"remaining backlog must be left to the standby that takes over")
}
