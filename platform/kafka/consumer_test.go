package kafka_test

import (
	"context"
	"fmt"
	"sync"
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
