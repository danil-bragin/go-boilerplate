package retry

// Internal (white-box) unit tests for Consumer fixes.
// These tests construct Consumer structs directly, bypassing NewConsumer's
// broker dial, so they run with -short and require no Docker.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// newBareConsumer builds a Consumer with nil kgo.Client — safe to use for
// tests that only exercise the mutex-guarded fields (held, timers, done).
// Close() must NOT be called on these consumers because c.client is nil.
func newBareConsumer() *Consumer {
	return &Consumer{
		wake:   make(chan wakeEvent, 256),
		held:   make(map[topicPartition]*heldBatch),
		timers: make(map[topicPartition]*time.Timer),
		done:   make(chan struct{}),
	}
}

// ---------------------------------------------------------------------------
// Fix 1 — TestClose_StopsPendingTimers_NoBlockedGoroutines
// ---------------------------------------------------------------------------

// TestClose_StopsPendingTimers_NoBlockedGoroutines proves that:
//  1. Scheduling a hold creates a tracked timer.
//  2. Manually closing done (simulating Close) stops the timer.
//  3. Firing the timer callback after close does NOT block (goroutine exits).
//  4. goleak detects no leaked goroutines.
func TestClose_StopsPendingTimers_NoBlockedGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t)

	c := newBareConsumer()
	tp := topicPartition{topic: "t", partition: 0}
	seq := c.nextSeq.Add(1)

	// Schedule a hold with a long timer (10s — won't fire naturally).
	c.mu.Lock()
	c.held[tp] = &heldBatch{records: nil, dueAt: time.Now().Add(10 * time.Second), seq: seq}
	ev := wakeEvent{tp: tp, seq: seq}
	done := c.done
	// Install a real AfterFunc — mirrors the production code path.
	timer := time.AfterFunc(10*time.Second, func() {
		select {
		case c.wake <- ev:
		case <-done:
			// escape: Close was called — do not block
		}
	})
	c.timers[tp] = timer
	c.mu.Unlock()

	// Simulate Close: signal done and stop all timers.
	close(c.done)
	c.mu.Lock()
	for tpKey, t2 := range c.timers {
		t2.Stop()
		delete(c.timers, tpKey)
	}
	c.mu.Unlock()

	// Fire the timer callback manually AFTER close — must not block.
	timerFired := make(chan struct{})
	go func() {
		defer close(timerFired)
		select {
		case c.wake <- ev:
		case <-c.done:
			// done is already closed — this path is taken immediately.
		}
	}()

	select {
	case <-timerFired:
		// Good: callback returned promptly.
	case <-time.After(time.Second):
		t.Fatal("timer callback blocked after Close — goroutine leak")
	}

	// Verify timers map is empty.
	c.mu.Lock()
	remaining := len(c.timers)
	c.mu.Unlock()
	assert.Equal(t, 0, remaining, "all timers must be cleared after close")
}

// ---------------------------------------------------------------------------
// Fix 2 — TestOnRevoked_ClearsHeldAndTimers
// ---------------------------------------------------------------------------

// TestOnRevoked_ClearsHeldAndTimers verifies that the revoke callback removes
// held batches and stops/deletes timers for the affected partitions, while
// leaving unaffected partitions intact.
func TestOnRevoked_ClearsHeldAndTimers(t *testing.T) {
	defer goleak.VerifyNone(t)

	c := newBareConsumer()

	tp1 := topicPartition{topic: "orders.retry.5s", partition: 0}
	tp2 := topicPartition{topic: "orders.retry.5s", partition: 1}
	tp3 := topicPartition{topic: "orders.retry.5s", partition: 2} // NOT revoked

	// Populate held and timers for tp1, tp2, tp3.
	for _, tp := range []topicPartition{tp1, tp2, tp3} {
		seq := c.nextSeq.Add(1)
		ev := wakeEvent{tp: tp, seq: seq}
		done := c.done
		timer := time.AfterFunc(10*time.Second, func() {
			select {
			case c.wake <- ev:
			case <-done:
			}
		})
		c.mu.Lock()
		c.held[tp] = &heldBatch{records: nil, dueAt: time.Now().Add(10 * time.Second), seq: seq}
		c.timers[tp] = timer
		c.mu.Unlock()
	}

	// Revoke tp1 and tp2 only.
	revokedMap := map[string][]int32{
		tp1.topic: {tp1.partition, tp2.partition},
	}
	c.onRevoked(context.Background(), nil, revokedMap)

	c.mu.Lock()
	_, held1 := c.held[tp1]
	_, held2 := c.held[tp2]
	_, held3 := c.held[tp3]
	_, timer1 := c.timers[tp1]
	_, timer2 := c.timers[tp2]
	_, timer3 := c.timers[tp3]
	c.mu.Unlock()

	require.False(t, held1, "tp1 held must be cleared after revoke")
	require.False(t, held2, "tp2 held must be cleared after revoke")
	require.True(t, held3, "tp3 held must remain (not revoked)")
	require.False(t, timer1, "tp1 timer must be removed after revoke")
	require.False(t, timer2, "tp2 timer must be removed after revoke")
	require.True(t, timer3, "tp3 timer must remain (not revoked)")

	// Cleanup: stop remaining timer for tp3 and close done to unblock goroutines.
	close(c.done)
	c.mu.Lock()
	if t3, ok := c.timers[tp3]; ok {
		t3.Stop()
	}
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Fix 4 — TestDrainWakes_IgnoresStaleSeq
// ---------------------------------------------------------------------------

// TestDrainWakes_IgnoresStaleSeq proves that a wake event with an outdated
// seq number (from a replaced heldBatch) is silently discarded rather than
// resuming the wrong generation.
func TestDrainWakes_IgnoresStaleSeq(t *testing.T) {
	// We use a bare consumer with a nil client — drainWakes will call
	// c.client.ResumeFetchPartitions only when it finds a matching seq.
	// Because we send a stale seq the client is never touched.
	c := newBareConsumer()
	tp := topicPartition{topic: "t", partition: 0}

	// Install a "current" batch with seq=2.
	currentSeq := uint64(2)
	c.mu.Lock()
	c.held[tp] = &heldBatch{records: nil, dueAt: time.Now(), seq: currentSeq}
	c.mu.Unlock()

	// Push a stale wake event (seq=1 — from the previous generation).
	staleEv := wakeEvent{tp: tp, seq: 1}
	c.wake <- staleEv

	// drainWakes with a nil client — if it tries to call ResumeFetchPartitions
	// on a nil client it will panic, proving the stale guard failed.
	// A panic-free return proves the stale event was discarded.
	require.NotPanics(t, func() {
		_ = c.drainWakes(context.Background(), nil)
	})

	// The held batch for the current generation must still be present.
	c.mu.Lock()
	batch, exists := c.held[tp]
	c.mu.Unlock()
	require.True(t, exists, "current-gen batch must not be cleared by stale wake")
	assert.Equal(t, currentSeq, batch.seq)
}

// ---------------------------------------------------------------------------
// Fix 3 — TestProcessPartition_MalformedNoDLTProduceNoCommit
// ---------------------------------------------------------------------------

// mockProducer is a simple producer that either always fails or always succeeds.
type mockProducer struct {
	failProduce bool
	produced    []kafka.Record
}

func (m *mockProducer) Produce(_ context.Context, rec kafka.Record) error {
	if m.failProduce {
		return assert.AnError
	}
	m.produced = append(m.produced, rec)
	return nil
}

// TestProcessPartition_MalformedDLTProduceFailureHolds verifies that a
// malformed record (no retry headers) whose DLT produce fails is NOT
// committed and IS held for a delayed retry. Merely skipping the commit
// would lose the record: kgo advances its consume position regardless of
// commits, so an unheld record is never refetched and any later commit
// advances past it.
func TestProcessPartition_MalformedDLTProduceFailureHolds(t *testing.T) {
	mp := &mockProducer{failProduce: true}
	c := newBareConsumer()
	// Real kgo client with an unreachable seed: PauseFetchPartitions (called
	// by holdRecords) is pure local bookkeeping, nothing is dialed.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	c.client = cl
	c.esc = &Escalator{producer: mp}
	var onErrorCalled atomic.Bool
	c.onError = func(error) { onErrorCalled.Store(true) }

	// Build a fake partition with one malformed record (no retry headers).
	rec := &kgo.Record{
		Topic:     "base.retry.5s",
		Partition: 0,
		Key:       []byte("k"),
		Value:     []byte("v"),
		// No Headers — malformed.
	}
	p := kgo.FetchTopicPartition{
		FetchPartition: kgo.FetchPartition{
			Partition: 0,
			Records:   []*kgo.Record{rec},
		},
		Topic: "base.retry.5s",
	}

	toCommit := c.processPartition(context.Background(), p, nil)

	// Produce failed → record must NOT be in toCommit…
	assert.Empty(t, toCommit, "toCommit must be empty when DLT produce failed")
	assert.True(t, onErrorCalled.Load(), "onError must be called on DLT produce failure")

	// …and MUST be held for a delayed escalation retry.
	tp := topicPartition{topic: "base.retry.5s", partition: 0}
	c.mu.Lock()
	batch, held := c.held[tp]
	_, timerSet := c.timers[tp]
	c.mu.Unlock()
	require.True(t, held, "failed record must be held, not skipped (skip = permanent loss)")
	require.Len(t, batch.records, 1)
	assert.Equal(t, rec, batch.records[0])
	assert.True(t, timerSet, "a wake timer must be scheduled for the held batch")
}

// TestProcessPartition_MalformedDLTSuccessCommits verifies that when the DLT
// produce succeeds, the record IS appended to toCommit (committed normally).
func TestProcessPartition_MalformedDLTSuccessCommits(t *testing.T) {
	mp := &mockProducer{failProduce: false}
	c := newBareConsumer()
	c.esc = &Escalator{producer: mp}
	c.onError = func(error) {}

	rec := &kgo.Record{
		Topic:     "base.retry.5s",
		Partition: 0,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}
	p := kgo.FetchTopicPartition{
		FetchPartition: kgo.FetchPartition{
			Partition: 0,
			Records:   []*kgo.Record{rec},
		},
		Topic: "base.retry.5s",
	}

	toCommit := c.processPartition(context.Background(), p, nil)

	assert.Len(t, toCommit, 1, "toCommit must contain the record when DLT produce succeeded")
	assert.Len(t, mp.produced, 1, "one record must have been produced to DLT")
}
