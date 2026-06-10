package kafka_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceManual produces records to explicit partitions using a manual partitioner.
func produceManual(t *testing.T, broker string, recs ...*kgo.Record) {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-manual-producer",
	}, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer cl.Close()
	for _, r := range recs {
		require.NoError(t, cl.ProduceSync(context.Background(), r).FirstErr())
	}
}

// TestRedeliveryOnHandlerFailure proves that a record whose handler fails
// transiently is redelivered (seek-back) and processed before any later record
// in the same partition — instead of being silently skipped and committed over.
func TestRedeliveryOnHandlerFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("redeliver-transient")
	group := uniqueName("g-transient")

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	produceManual(
		t, broker,
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r1"), Value: []byte("v1")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r2"), Value: []byte("v2")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r3"), Value: []byte("v3")},
	)

	c, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-transient", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)
	defer c.Close(ctx)

	var (
		mu        sync.Mutex
		processed []string // keys in successful-processing order
		r2Fails   int
		done      = make(chan struct{})
	)

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	go func() {
		_ = c.Run(runCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			defer mu.Unlock()
			if string(r.Key) == "r2" && r2Fails < 2 {
				r2Fails++
				return errors.New("transient failure")
			}
			processed = append(processed, string(r.Key))
			if len(processed) == 3 {
				close(done)
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-runCtx.Done():
		mu.Lock()
		t.Fatalf("timed out; processed=%v r2Fails=%d — r2 was lost, not redelivered", processed, r2Fails)
	}
	cancel()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"r1", "r2", "r3"}, processed,
		"per-partition order must be preserved across redelivery")
	require.Equal(t, 2, r2Fails)
}

// TestNoCommitPastFailure proves the consumer never commits offsets past a
// permanently failing record: a second consumer in the same group must see the
// failed record again, not resume after it.
func TestNoCommitPastFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("redeliver-permanent")
	group := uniqueName("g-perm")

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	produceManual(
		t, broker,
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r1"), Value: []byte("v1")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r2"), Value: []byte("v2")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("r3"), Value: []byte("v3")},
	)

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-perm-1", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)

	var (
		mu       sync.Mutex
		attempts int
		enough   = make(chan struct{})
		once     sync.Once
	)

	run1Ctx, cancel1 := context.WithCancel(ctx)
	run1Done := make(chan struct{})
	go func() {
		defer close(run1Done)
		_ = c1.Run(run1Ctx, func(_ context.Context, r kafka.Record) error {
			if string(r.Key) == "r2" {
				mu.Lock()
				attempts++
				n := attempts
				mu.Unlock()
				if n >= 3 {
					once.Do(func() { close(enough) })
				}
				return errors.New("permanent failure")
			}
			return nil
		})
	}()

	select {
	case <-enough:
	case <-time.After(60 * time.Second):
		mu.Lock()
		t.Fatalf("r2 attempted %d times in 60s — no redelivery happening (record lost)", attempts)
	}
	cancel1()
	<-run1Done
	require.NoError(t, c1.Close(ctx))

	// Second consumer, same group: must receive r2 first (commit stopped at r1).
	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-perm-2", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)
	defer c2.Close(ctx)

	firstKey := make(chan string, 1)
	run2Ctx, cancel2 := context.WithTimeout(ctx, 60*time.Second)
	defer cancel2()
	go func() {
		_ = c2.Run(run2Ctx, func(_ context.Context, r kafka.Record) error {
			select {
			case firstKey <- string(r.Key):
			default:
			}
			return nil
		})
	}()

	select {
	case k := <-firstKey:
		require.Equal(t, "r2", k, "committed offset must not have advanced past the failed record")
	case <-run2Ctx.Done():
		t.Fatal("second consumer received nothing — nothing left to redeliver: r2 lost AND committed past")
	}
}

// TestFailureIsolatedPerPartition proves a poison record on one partition does
// not stop progress on other partitions.
func TestFailureIsolatedPerPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("redeliver-isolated")
	group := uniqueName("g-iso")

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 2, ReplicationFactor: 1}, topic))

	recs := []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("poison"), Value: []byte("p")},
	}
	for i := 0; i < 5; i++ {
		recs = append(recs, &kgo.Record{
			Topic: topic, Partition: 1,
			Key: []byte("ok" + strconv.Itoa(i)), Value: []byte("v"),
		})
	}
	produceManual(t, broker, recs...)

	c, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "c-iso", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)
	defer c.Close(ctx)

	var (
		mu     sync.Mutex
		okSeen = map[string]bool{}
		allOK  = make(chan struct{})
		okOnce sync.Once
	)

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	go func() {
		_ = c.Run(runCtx, func(_ context.Context, r kafka.Record) error {
			if string(r.Key) == "poison" {
				return errors.New("poison")
			}
			mu.Lock()
			okSeen[string(r.Key)] = true
			n := len(okSeen)
			mu.Unlock()
			if n == 5 {
				okOnce.Do(func() { close(allOK) })
			}
			return nil
		})
	}()

	select {
	case <-allOK:
	case <-runCtx.Done():
		mu.Lock()
		t.Fatalf("partition 1 starved by poison on partition 0; got %d/5", len(okSeen))
	}
}
