package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/kafka/kafkatest"
)

func TestConsumer_GroupConsumesCommitted(t *testing.T) {
	broker, _ := kafkatest.NewRedpanda(t)

	const (
		topic   = "ctopic"
		groupID = "g1"
		n       = 5
	)

	ctx := context.Background()

	// ── Setup: create topic and produce N records ────────────────────────────

	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, 1, 1, topic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		err = prod.Produce(ctx, kafka.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		})
		require.NoError(t, err)
	}
	require.NoError(t, prod.Close(ctx))

	// ── First consumer: collect all N records ───────────────────────────────

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-consumer-1",
		GroupID:  groupID,
	}, topic)
	require.NoError(t, err)

	var (
		mu       sync.Mutex
		received []string
		done     = make(chan struct{})
	)

	runCtx, cancelRun := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRun()

	go func() {
		_ = c1.Run(runCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			received = append(received, string(r.Key))
			if len(received) >= n {
				close(done)
			}
			mu.Unlock()
			return nil
		})
	}()

	// Wait until all N records are collected or test times out.
	select {
	case <-done:
		// All records received; give CommitRecords a moment to complete before
		// we cancel the context and close the consumer.
		time.Sleep(200 * time.Millisecond)
	case <-runCtx.Done():
		t.Fatal("timed out waiting for all records in first consumer")
	}

	cancelRun()
	c1.Close()

	mu.Lock()
	keys := make([]string, len(received))
	copy(keys, received)
	mu.Unlock()

	require.Len(t, keys, n)
	for i := 0; i < n; i++ {
		assert.Contains(t, keys, fmt.Sprintf("%d", i), "missing key %d", i)
	}

	// ── Second consumer in the SAME group: should see zero records ───────────
	// All offsets were committed by the first consumer, so nothing is
	// outstanding for the group.  A small redelivery window is theoretically
	// possible (e.g. a crash between handle and commit), but with a clean
	// context cancel + Close the offsets should be durable.

	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-consumer-2",
		GroupID:  groupID,
	}, topic)
	require.NoError(t, err)

	var (
		mu2       sync.Mutex
		received2 []string
	)

	restartCtx, cancelRestart := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRestart()

	_ = c2.Run(restartCtx, func(_ context.Context, r kafka.Record) error {
		mu2.Lock()
		received2 = append(received2, string(r.Key))
		mu2.Unlock()
		return nil
	})

	c2.Close()

	mu2.Lock()
	n2 := len(received2)
	mu2.Unlock()

	// Assert zero redeliveries.  If the assertion is flaky in practice,
	// weaken to assert.Empty(t, received2, "expected no redelivery").
	assert.Equal(t, 0, n2, "second consumer in same group should receive 0 records; got %d", n2)
}

// TestConsumer_HandlerErrorRedeliversRecord verifies the at-least-once
// redelivery contract of Consumer.Run: when the handler returns an error the
// offset is NOT committed, so closing the consumer and starting a new one
// in the same group causes the broker to redeliver the record from the last
// committed offset.
//
// Redelivery in this implementation happens at the session boundary, not
// within a single Run call. If the handler fails and returns an error, the
// record's offset is not committed. When the consumer group session ends
// (consumer closed / context cancelled), the broker re-delivers the record
// to the next consumer that joins the same group.
//
// Per-record offset commit means there is a small duplicate window around a
// rebalance: if the process crashes between a successful handler call and the
// CommitRecords RPC completing, the record will be redelivered after the
// partition is reassigned. Consumers MUST be idempotent and deduplicate by a
// stable idempotency key (e.g. an inbox table keyed on message-id).
func TestConsumer_HandlerErrorRedeliversRecord(t *testing.T) {
	broker, _ := kafkatest.NewRedpanda(t)

	const (
		topic   = "redeliver-topic"
		groupID = "redeliver-group"
	)

	ctx := context.Background()

	// Setup: create topic and produce 1 record.
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "redeliver-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, 1, 1, topic))

	prod := kafka.NewProducer(adminCl)
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: topic,
		Key:   []byte("rk"),
		Value: []byte("rv"),
	}))
	require.NoError(t, prod.Close(ctx))

	// Phase 1: consumer that FAILS the handler.
	// The record offset is not committed; after Close the broker will
	// redeliver the record to the next consumer joining the same group.
	var firstSawRecord atomic.Bool

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "redeliver-consumer-1",
		GroupID:  groupID,
	}, topic)
	require.NoError(t, err)

	c1Ctx, cancelC1 := context.WithTimeout(ctx, 30*time.Second)
	defer cancelC1()

	c1Done := make(chan struct{})
	go func() {
		defer close(c1Done)
		_ = c1.Run(c1Ctx, func(_ context.Context, _ kafka.Record) error {
			firstSawRecord.Store(true)
			cancelC1() // cancel after first failed call so we can move on
			return errors.New("intentional first-pass failure")
		})
	}()

	select {
	case <-c1Done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for first consumer to see the record")
	}
	c1.Close()

	require.True(t, firstSawRecord.Load(), "first consumer must have received the record")

	// Phase 2: consumer that SUCCEEDS.
	// Because phase 1 did not commit the offset, the broker redelivers the record.
	// Use a fresh context (not derived from c1Ctx which is already cancelled).
	var secondCallCount atomic.Int32
	c2Succeeded := make(chan struct{})
	var c2SuccessOnce sync.Once

	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "redeliver-consumer-2",
		GroupID:  groupID,
	}, topic)
	require.NoError(t, err)

	c2Ctx, cancelC2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelC2()

	c2Done := make(chan struct{})
	go func() {
		defer close(c2Done)
		_ = c2.Run(c2Ctx, func(_ context.Context, _ kafka.Record) error {
			secondCallCount.Add(1)
			c2SuccessOnce.Do(func() {
				close(c2Succeeded)
			})
			cancelC2() // stop after receiving the redelivered record
			return nil
		})
	}()

	select {
	case <-c2Succeeded:
		// Record redelivered and successfully processed.
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for redelivery on second consumer")
	}

	cancelC2()
	<-c2Done
	c2.Close()

	// The second consumer must have received the record at least once,
	// proving the broker redelivered the uncommitted offset from phase 1.
	assert.GreaterOrEqual(t, int(secondCallCount.Load()), 1,
		"second consumer must receive the redelivered record at least once")
}
