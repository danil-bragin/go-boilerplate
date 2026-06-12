package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// values extracts the string Values of a batch for diagnostic messages.
func values(recs []kafka.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}

// TestRunBatch_DeliversPartitionRecordsAndCommits proves the happy-path
// BatchHandlerFunc contract: every record produced to a single partition is
// delivered to the handler in offset order, and once the handler reports the
// whole batch applied (processed == len(recs)) the offsets are committed — a
// second consumer in the same group sees zero redeliveries.
func TestRunBatch_DeliversPartitionRecordsAndCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("batch-commit-topic")
	group := uniqueName("batch-commit-group")
	const n = 8

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "batch-admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		require.NoError(t, prod.Produce(ctx, kafka.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}))
	}
	require.NoError(t, prod.Close(ctx))

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "batch-consumer-1", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)

	var (
		mu       sync.Mutex
		received []string
		done     = make(chan struct{})
		doneOnce sync.Once
	)

	runCtx, cancelRun := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRun()

	go func() {
		_ = c1.RunBatch(runCtx, func(_ context.Context, recs []kafka.Record) (int, error) {
			mu.Lock()
			for _, r := range recs {
				received = append(received, string(r.Value))
			}
			total := len(received)
			mu.Unlock()
			if total >= n {
				doneOnce.Do(func() { close(done) })
			}
			// Whole batch applied.
			return len(recs), nil
		})
	}()

	select {
	case <-done:
		// Give CommitRecords a moment to land before cancelling.
		time.Sleep(300 * time.Millisecond)
	case <-runCtx.Done():
		t.Fatal("timed out waiting for batch handler to receive all records")
	}

	cancelRun()
	_ = c1.Close(ctx)

	mu.Lock()
	got := make([]string, len(received))
	copy(got, received)
	mu.Unlock()

	// All N records delivered, in offset order (single partition).
	require.Len(t, got, n)
	for i := 0; i < n; i++ {
		assert.Equal(t, fmt.Sprintf("v%d", i), got[i], "record %d delivered out of order", i)
	}

	// Second consumer, same group: offsets were committed, so zero redeliveries.
	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "batch-consumer-2", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)

	var (
		mu2       sync.Mutex
		received2 int
	)
	restartCtx, cancelRestart := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRestart()

	_ = c2.RunBatch(restartCtx, func(_ context.Context, recs []kafka.Record) (int, error) {
		mu2.Lock()
		received2 += len(recs)
		mu2.Unlock()
		return len(recs), nil
	})
	_ = c2.Close(ctx)

	mu2.Lock()
	n2 := received2
	mu2.Unlock()
	assert.Equal(t, 0, n2, "second consumer in same group should receive 0 records; got %d", n2)
}

// TestRunBatch_PartialFailureSeeksBack proves the partial-failure contract: a
// batch handler that applies the first 2 records and then fails (returns
// (2, err)) must commit records[0] and records[1] and seek back to records[2]
// so it is redelivered. No record is lost; records[2] (and the rest) reappear
// on a later poll. We assert the handler's later call(s) start at records[2]
// AND that a fresh consumer in the same group resumes at records[2], proving
// the commit stopped exactly at the first unprocessed record.
func TestRunBatch_PartialFailureSeeksBack(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("batch-partial-topic")
	group := uniqueName("batch-partial-group")
	const n = 5 // values "v0".."v4" on a single partition

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "batch-partial-admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		require.NoError(t, prod.Produce(ctx, kafka.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}))
	}
	require.NoError(t, prod.Close(ctx))

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "batch-partial-consumer-1", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)

	var (
		mu             sync.Mutex
		calls          [][]string // value-slice each handler call observed
		failedOnce     bool
		v2Redelivered  = make(chan struct{})
		redeliverOnce  sync.Once
		appliedInOrder []string // values committed (processed) in order
	)

	runCtx, cancel1 := context.WithTimeout(ctx, 60*time.Second)
	defer cancel1()

	go func() {
		_ = c1.RunBatch(runCtx, func(_ context.Context, recs []kafka.Record) (int, error) {
			vals := make([]string, len(recs))
			for i, r := range recs {
				vals[i] = string(r.Value)
			}
			mu.Lock()
			calls = append(calls, vals)
			mu.Unlock()

			// First non-empty batch: apply 2, fail. The first batch begins at
			// v0, so we commit v0,v1 and seek back to v2.
			mu.Lock()
			isFirst := !failedOnce
			if isFirst {
				failedOnce = true
			}
			mu.Unlock()

			if isFirst {
				require.GreaterOrEqual(t, len(recs), 3,
					"first poll must deliver at least v0,v1,v2 to exercise the seek-back")
				require.Equal(t, "v0", vals[0], "first batch must start at v0")
				mu.Lock()
				appliedInOrder = append(appliedInOrder, vals[0], vals[1])
				mu.Unlock()
				return 2, errors.New("intentional partial-batch failure after 2")
			}

			// Subsequent batches: must start at the redelivered v2 (seek-back
			// target), proving no record was lost and v2 is redelivered.
			require.Equal(t, "v2", vals[0],
				"redelivered batch must start at the sought-back record v2; got %v", vals)
			redeliverOnce.Do(func() { close(v2Redelivered) })
			mu.Lock()
			appliedInOrder = append(appliedInOrder, vals...)
			mu.Unlock()
			return len(recs), nil
		})
	}()

	select {
	case <-v2Redelivered:
		time.Sleep(300 * time.Millisecond) // let the final commit land
	case <-runCtx.Done():
		mu.Lock()
		t.Fatalf("v2 never redelivered after partial failure — record lost. calls=%v", calls)
	}
	cancel1()
	_ = c1.Close(ctx)

	mu.Lock()
	require.GreaterOrEqual(t, len(calls), 2, "expected at least an initial + a redelivery poll")
	// v2 must reappear as the head of a later batch (the seek-back target).
	assert.Equal(t, "v2", calls[len(calls)-1][0], "last poll must begin at v2")
	// All five values must ultimately have been applied, v2 included.
	seen := map[string]bool{}
	for _, v := range appliedInOrder {
		seen[v] = true
	}
	mu.Unlock()
	for i := 0; i < n; i++ {
		assert.True(t, seen[fmt.Sprintf("v%d", i)], "v%d must have been applied (not lost)", i)
	}
}

// TestRunBatch_NoCommitPastFailure proves the commit never advances past a
// permanently failing record: a batch handler that always applies only the
// first 2 records and fails must leave records[2..] uncommitted, so a second
// consumer in the same group resumes exactly at records[2] — records[0] and
// records[1] are committed and never redelivered, records[2] is never lost.
func TestRunBatch_NoCommitPastFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	topic := uniqueName("batch-nocommit-topic")
	group := uniqueName("batch-nocommit-group")
	const n = 5

	adminCl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "batch-nocommit-admin"})
	require.NoError(t, err)
	defer adminCl.Close()
	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		require.NoError(t, prod.Produce(ctx, kafka.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}))
	}
	require.NoError(t, prod.Close(ctx))

	c1, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "batch-nocommit-consumer-1", GroupID: group,
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
		_ = c1.RunBatch(run1Ctx, func(_ context.Context, recs []kafka.Record) (int, error) {
			// Apply every record up to (not including) the poison v2, then fail
			// ON v2. Pinning the failure to a stable record means the commit can
			// never advance past it however many polls happen: v0,v1 commit,
			// v2 is sought back every time.
			processed := 0
			for processed < len(recs) && string(recs[processed].Value) != "v2" {
				processed++
			}
			require.Less(t, processed, len(recs),
				"every poll must still contain the poison v2 — commit advanced past the failure (record lost). got %v", values(recs))
			mu.Lock()
			attempts++
			a := attempts
			mu.Unlock()
			if a >= 3 {
				once.Do(func() { close(enough) })
			}
			return processed, errors.New("permanent failure on v2")
		})
	}()

	select {
	case <-enough:
	case <-time.After(60 * time.Second):
		mu.Lock()
		t.Fatalf("batch failed-record attempted %d times in 60s — no redelivery (record lost)", attempts)
	}
	cancel1()
	<-run1Done
	require.NoError(t, c1.Close(ctx))

	// Second consumer, same group: v0,v1 were committed, so it must resume at
	// v2 (the seek-back target) — never before, never skipping it.
	c2, err := kafka.NewConsumer(kafka.Config{
		Brokers: []string{broker}, ClientID: "batch-nocommit-consumer-2", GroupID: group,
	}, []string{topic})
	require.NoError(t, err)
	defer c2.Close(ctx)

	firstVal := make(chan string, 1)
	run2Ctx, cancel2 := context.WithTimeout(ctx, 60*time.Second)
	defer cancel2()
	go func() {
		_ = c2.RunBatch(run2Ctx, func(_ context.Context, recs []kafka.Record) (int, error) {
			select {
			case firstVal <- string(recs[0].Value):
			default:
			}
			return len(recs), nil
		})
	}()

	select {
	case v := <-firstVal:
		require.Equal(t, "v2", v,
			"committed offset must stop exactly at the failed record: second consumer must resume at v2")
	case <-run2Ctx.Done():
		t.Fatal("second consumer received nothing — v2 lost AND committed past")
	}
}
