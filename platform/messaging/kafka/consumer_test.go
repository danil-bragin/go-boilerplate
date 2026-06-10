package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/testkit/goleakopts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

func TestConsumer_GroupConsumesCommitted(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	// Two consumer Run goroutines are started and closed in this test; the
	// leak check proves Run/Close leave nothing behind (client-lifetime kgo
	// loops and shared-container goroutines are ignored via goleakopts).
	defer goleak.VerifyNone(t, goleakopts.Default(goleak.IgnoreCurrent())...)

	topic := uniqueName("ctopic")
	groupID := uniqueName("g1")
	const n = 5

	ctx := context.Background()

	// ── Setup: create topic and produce N records ────────────────────────────

	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		err = prod.Produce(ctx, kafka.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(i)),
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
	}, []string{topic})
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
	_ = c1.Close(ctx)

	mu.Lock()
	keys := make([]string, len(received))
	copy(keys, received)
	mu.Unlock()

	require.Len(t, keys, n)
	for i := 0; i < n; i++ {
		assert.Contains(t, keys, strconv.Itoa(i), "missing key %d", i)
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
	}, []string{topic})
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

	_ = c2.Close(ctx)

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
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	topic := uniqueName("redeliver-topic")
	groupID := uniqueName("redeliver-group")

	ctx := context.Background()

	// Setup: create topic and produce 1 record.
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "redeliver-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

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
	}, []string{topic})
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
	_ = c1.Close(ctx)

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
	}, []string{topic})
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
	_ = c2.Close(context.Background())

	// The second consumer must have received the record at least once,
	// proving the broker redelivered the uncommitted offset from phase 1.
	assert.GreaterOrEqual(t, int(secondCallCount.Load()), 1,
		"second consumer must receive the redelivered record at least once")
}

// TestConsumer_PerPartitionParallelOrdering verifies that:
//  1. Within each partition, records are processed in order (no reordering).
//  2. Partitions are processed concurrently: total wall-time is significantly
//     less than the serial upper bound (nRecords * sleepPerRecord).
//
// Three partitions are created and records are routed explicitly via
// ManualPartitioner. Each record carries a monotonically increasing sequence
// number per partition. The handler sleeps a few ms to make parallel speedup
// observable, then records the sequence in the order it was called. After all
// records are consumed the test asserts (a) each partition's sequences are in
// ascending order and (b) total elapsed < nPartitions * nPerPartition *
// sleepPerRecord * 0.7 (i.e. ≥30 % faster than fully serial).
func TestConsumer_PerPartitionParallelOrdering(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	topic := uniqueName("parallel-ordering-topic")
	groupID := uniqueName("parallel-ordering-group")
	const (
		nPartitions    = 3
		nPerPartition  = 4
		sleepPerRecord = 10 * time.Millisecond
	)

	ctx := context.Background()

	// Admin client: create the topic with nPartitions partitions.
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "parallel-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: nPartitions, ReplicationFactor: 1}, topic))

	// Produce nPerPartition records to each partition with explicit partition
	// assignment. Key encodes "p<partition>-<seq>" for later validation.
	rawCl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer rawCl.Close()

	for p := int32(0); p < nPartitions; p++ {
		for seq := 0; seq < nPerPartition; seq++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: p,
				Key:       []byte(fmt.Sprintf("p%d", p)),
				Value:     []byte(strconv.Itoa(seq)),
			}
			if err := rawCl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				t.Fatalf("produce to partition %d seq %d: %v", p, seq, err)
			}
		}
	}

	// consumed tracks the order records were processed: partition → []seq.
	// firstRecordAt is set atomically when the very first record is handed to
	// the handler; this excludes consumer-group-join latency from the timing
	// assertion so CI overhead does not cause false failures.
	var (
		mu            sync.Mutex
		consumed      = make(map[int32][]int)
		total         = int32(0)
		allDone       = make(chan struct{})
		firstRecordAt atomic.Pointer[time.Time]
	)
	const nTotal = nPartitions * nPerPartition

	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "parallel-ordering-consumer",
		GroupID:  groupID,
	}, []string{topic})
	require.NoError(t, err)

	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRun()

	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r kafka.Record) error {
			// Record wall-clock when first record enters the handler.
			now := time.Now()
			firstRecordAt.CompareAndSwap(nil, &now)

			// Simulate work per record.
			time.Sleep(sleepPerRecord)

			var seq int
			fmt.Sscanf(string(r.Value), "%d", &seq)

			// Determine partition from the key ("p0", "p1", "p2").
			var part int32
			fmt.Sscanf(string(r.Key)[1:], "%d", &part)

			mu.Lock()
			consumed[part] = append(consumed[part], seq)
			n := atomic.AddInt32(&total, 1)
			mu.Unlock()

			if n >= nTotal {
				select {
				case <-allDone:
				default:
					close(allDone)
				}
			}
			return nil
		})
	}()

	select {
	case <-allDone:
	case <-runCtx.Done():
		t.Fatal("timed out waiting for all records")
	}

	// Measure processing duration from first-record-entered to allDone.
	// This deliberately excludes group-join latency, which varies with
	// container startup and has nothing to do with parallelism.
	startPtr := firstRecordAt.Load()
	require.NotNil(t, startPtr, "firstRecordAt must be set")
	elapsed := time.Since(*startPtr)

	cancelRun()
	_ = consumer.Close(ctx)

	// (a) Per-partition ordering: sequences must be in ascending order.
	mu.Lock()
	defer mu.Unlock()

	for p := int32(0); p < nPartitions; p++ {
		seqs := consumed[p]
		require.Len(t, seqs, nPerPartition, "partition %d: wrong record count", p)
		sorted := make([]int, len(seqs))
		copy(sorted, seqs)
		sort.Ints(sorted)
		assert.Equal(t, sorted, seqs, "partition %d: records out of order: %v", p, seqs)
	}

	// (b) Parallel speedup: fully serial processing would take
	//     nPartitions * nPerPartition * sleepPerRecord (= 120ms here).
	//     With 3 concurrent partitions the ideal is ~nPerPartition*sleep (= 40ms).
	//     We assert elapsed < 70% of serial to prove overlap, using only
	//     in-handler time (group-join latency excluded above).
	serialBound := time.Duration(nPartitions*nPerPartition) * sleepPerRecord
	parallelBound := serialBound * 70 / 100
	assert.Less(t, elapsed, parallelBound,
		"processing elapsed %v should be < 70%% of serial bound %v — partitions may not be running concurrently",
		elapsed, parallelBound)
}

// TestConsumer_OnePartitionFailureDoesNotBlockOthers verifies per-partition
// failure isolation: a handler that always fails for one partition's records
// must not prevent the other partition's records from being committed.
//
// Setup: 2-partition topic. Partition 0 records fail the handler forever;
// partition 1 records always succeed. After the healthy partition has been
// fully consumed, the context is cancelled. A second consumer (same group)
// then joins and should see zero records from partition 1 (all committed) but
// at least one redelivery from partition 0 (never committed).
func TestConsumer_OnePartitionFailureDoesNotBlockOthers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	topic := uniqueName("partition-isolation-topic")
	groupID := uniqueName("partition-isolation-group")
	const (
		nHealthy      = 3
		healthyPartID = int32(1)
		failingPartID = int32(0)
	)

	ctx := context.Background()

	// Admin setup.
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "isolation-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 2, ReplicationFactor: 1}, topic))

	// Produce to specific partitions.
	rawCl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer rawCl.Close()

	// One record on the failing partition.
	require.NoError(t, rawCl.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Partition: failingPartID,
		Key:       []byte("fail"),
		Value:     []byte("should-never-commit"),
	}).FirstErr())

	// nHealthy records on the healthy partition.
	for i := 0; i < nHealthy; i++ {
		require.NoError(t, rawCl.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: healthyPartID,
			Key:       []byte("ok"),
			Value:     []byte(fmt.Sprintf("healthy-%d", i)),
		}).FirstErr())
	}

	// Phase 1: consume; healthy partition succeeds, failing partition always errors.
	var (
		healthyCount atomic.Int32
		healthyDone  = make(chan struct{})
		healthyOnce  sync.Once
	)

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "isolation-consumer-1",
		GroupID:  groupID,
	}, []string{topic})
	require.NoError(t, err)

	c1Ctx, cancelC1 := context.WithTimeout(ctx, 60*time.Second)
	defer cancelC1()

	c1Done := make(chan struct{})
	go func() {
		defer close(c1Done)
		_ = c1.Run(c1Ctx, func(_ context.Context, r kafka.Record) error {
			if string(r.Key) == "fail" {
				return errors.New("intentional failure")
			}
			n := healthyCount.Add(1)
			if int(n) >= nHealthy {
				healthyOnce.Do(func() { close(healthyDone) })
			}
			return nil
		})
	}()

	// Wait until all healthy records are processed.
	select {
	case <-healthyDone:
		// Give CommitRecords a moment to land before cancelling.
		time.Sleep(300 * time.Millisecond)
	case <-c1Ctx.Done():
		t.Fatal("timed out waiting for healthy partition records to be consumed")
	}

	cancelC1()
	<-c1Done
	_ = c1.Close(ctx)

	assert.GreaterOrEqual(t, int(healthyCount.Load()), nHealthy,
		"healthy partition should have processed all %d records", nHealthy)

	// Phase 2: second consumer in same group.
	// Healthy partition: 0 redeliveries (all committed).
	// Failing partition: ≥1 redelivery (never committed).
	var (
		mu2         sync.Mutex
		redelivered = make(map[string]int) // key → count
	)

	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "isolation-consumer-2",
		GroupID:  groupID,
	}, []string{topic})
	require.NoError(t, err)

	c2Ctx, cancelC2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelC2()

	_ = c2.Run(c2Ctx, func(_ context.Context, r kafka.Record) error {
		mu2.Lock()
		redelivered[string(r.Key)]++
		mu2.Unlock()
		return nil
	})
	_ = c2.Close(context.Background())

	mu2.Lock()
	okCount := redelivered["ok"]
	failCount := redelivered["fail"]
	mu2.Unlock()

	assert.Equal(t, 0, okCount,
		"healthy partition must have 0 redeliveries; got %d", okCount)
	assert.GreaterOrEqual(t, failCount, 1,
		"failing partition must redeliver at least 1 record; got %d", failCount)
}
