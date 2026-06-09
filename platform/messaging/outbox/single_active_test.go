package outbox_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// twoPoolsSharedSchema starts ONE postgres container and returns two
// independent pg.Pools connected to the same database (simulating two relay
// instances of the same service).
func twoPoolsSharedSchema(t *testing.T) (*pg.Pool, *pg.Pool) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool1, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool1.Close(ctx) })
	pool2, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool2.Close(ctx) })
	return pool1, pool2
}

// enqueueSeq enqueues n messages for one aggregate with sequence numbers
// startSeq..startSeq+n-1 encoded in the payload, so ordering can be asserted.
func enqueueSeq(t *testing.T, pool *pg.Pool, aggregateID string, startSeq, n int) {
	t.Helper()
	ctx := context.Background()
	repo := outbox.NewRepository(pool)
	for i := 0; i < n; i++ {
		seq := startSeq + i
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID:            uuid.New(),
				AggregateType: "order",
				AggregateID:   aggregateID,
				EventType:     "OrderCreated",
				Payload:       []byte(strconv.Itoa(seq)),
			})
		}))
	}
}

// seqsOf extracts the integer sequence numbers from published payloads.
func seqsOf(msgs []outbox.Message) []int {
	out := make([]int, 0, len(msgs))
	for _, m := range msgs {
		n, err := strconv.Atoi(string(m.Payload))
		if err == nil {
			out = append(out, n)
		}
	}
	return out
}

// TestRelay_SingleActive_OnlyLeaderPublishes_FailoverPreservesOrder verifies:
//  1. With WithSingleActive, only ONE of two concurrently-running relays
//     publishes (the standby stays idle).
//  2. When the leader's advisory-lock session dies (crash simulated via
//     pg_terminate_backend) the standby takes over within a few intervals.
//  3. Per-aggregate ordering is preserved across the failover: all pre-failover
//     sequence numbers are published by the old leader in order, all
//     post-failover ones by the new leader in order.
func TestRelay_SingleActive_OnlyLeaderPublishes_FailoverPreservesOrder(t *testing.T) {
	const interval = 50 * time.Millisecond
	pool1, pool2 := twoPoolsSharedSchema(t)
	ctx := context.Background()

	pub1, pub2 := &fakeBatchPublisher{}, &fakeBatchPublisher{}
	cfg := outbox.RelayConfig{BatchSize: 100, PollInterval: interval}
	relay1 := outbox.NewRelay(pool1, pub1, cfg, outbox.WithSingleActive(pool1.Writer()))
	relay2 := outbox.NewRelay(pool2, pub2, cfg, outbox.WithSingleActive(pool2.Writer()))

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() { defer close(done1); _ = relay1.Run(ctx1) }()
	go func() { defer close(done2); _ = relay2.Run(ctx2) }()

	// Phase 1: enqueue 10 messages; exactly one relay must publish them all.
	enqueueSeq(t, pool1, "agg-1", 0, 10)
	require.Eventually(t, func() bool { return pub1.count()+pub2.count() == 10 },
		10*time.Second, 10*time.Millisecond, "phase-1 messages must be published")
	// Give the standby a couple of intervals to (incorrectly) publish dupes.
	time.Sleep(4 * interval)

	require.True(t, (pub1.count() == 10 && pub2.count() == 0) ||
		(pub2.count() == 10 && pub1.count() == 0),
		"exactly one relay must publish: pub1=%d pub2=%d", pub1.count(), pub2.count())

	leaderPub, standbyPub := pub1, pub2
	leaderCancel := cancel1
	leaderDone := done1
	if pub2.count() == 10 {
		leaderPub, standbyPub = pub2, pub1
		leaderCancel = cancel2
		leaderDone = done2
	}

	// Phase 2: crash the leader — terminate the backend session holding the
	// advisory lock AND stop the leader relay (process death simulation).
	leaderCancel()
	<-leaderDone
	_, err := pool1.Writer().Exec(ctx, `
		select pg_terminate_backend(l.pid)
		from pg_locks l
		where l.locktype = 'advisory' and l.granted
		  and l.pid <> pg_backend_pid()`)
	require.NoError(t, err)

	enqueueSeq(t, pool1, "agg-1", 10, 10)
	require.Eventually(t, func() bool { return standbyPub.count() == 10 },
		10*time.Second, 10*time.Millisecond,
		"standby must take over after leader death (got %d)", standbyPub.count())

	// Phase 3: ordering. Old leader published 0..9 in order, new leader 10..19.
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, seqsOf(leaderPub.messages()),
		"pre-failover messages must be in per-aggregate order")
	require.Equal(t, []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, seqsOf(standbyPub.messages()),
		"post-failover messages must be in per-aggregate order")
}
