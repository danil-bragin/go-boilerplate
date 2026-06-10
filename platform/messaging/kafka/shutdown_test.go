// appended to redelivery_test.go logically; separate file
package kafka_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/testkit/goleakopts"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// TestShutdownCommitsProcessedRecords proves that records successfully
// processed in the poll during which the run context is cancelled are still
// committed (with a fresh detached context) — a restart must NOT redeliver
// them. Without the fix the final CommitRecords runs with the already-
// cancelled context, fails silently, and every deploy causes a redelivery
// storm of the last poll's records.
func TestShutdownCommitsProcessedRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	// Both consumers Run in goroutines and are closed below; nothing of the
	// shutdown path may outlive the test (kgo client-lifetime loops and the
	// shared-container goroutines are ignored via goleakopts).
	defer goleak.VerifyNone(t, goleakopts.Default(goleak.IgnoreCurrent())...)

	topic := uniqueName("shutdown-commit")
	group := uniqueName("g-shut")

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	var recs []*kgo.Record
	for i := 0; i < 5; i++ {
		recs = append(recs, &kgo.Record{
			Topic: topic, Partition: 0,
			Key: []byte("k" + strconv.Itoa(i)), Value: []byte("v"),
		})
	}
	produceManual(t, broker, recs...)

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-shut-1", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)

	// Cancel the run context from INSIDE the handler on the last record:
	// the cancellation is observed by Run after the batch is processed,
	// exactly the shutdown-mid-batch shape.
	run1Ctx, cancel1 := context.WithCancel(ctx)
	var processed int
	var mu sync.Mutex
	run1Done := make(chan struct{})
	go func() {
		defer close(run1Done)
		_ = c1.Run(run1Ctx, func(_ context.Context, _ kafka.Record) error {
			mu.Lock()
			processed++
			if processed == 5 {
				cancel1()
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case <-run1Done:
	case <-time.After(60 * time.Second):
		t.Fatal("consumer did not stop after cancel")
	}
	require.NoError(t, c1.Close(ctx))

	// Second consumer, same group: nothing must be redelivered.
	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-shut-2", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)
	defer c2.Close(ctx)

	redelivered := make(chan string, 8)
	run2Ctx, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	go func() {
		_ = c2.Run(run2Ctx, func(_ context.Context, r kafka.Record) error {
			redelivered <- string(r.Key)
			return nil
		})
	}()

	select {
	case k := <-redelivered:
		t.Fatalf("record %q redelivered after clean shutdown — final commit was dropped", k)
	case <-run2Ctx.Done():
		// 15s of silence: offsets were committed before shutdown. Success.
	}
}
