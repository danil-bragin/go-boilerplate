package kafka_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransact_ExactlyOnceHappyPath verifies the happy-path EOS contract:
// exactly 5 input records produce exactly 5 output records (no duplicates)
// and offsets are committed so a fresh consumer group session sees no
// redelivery.
func TestTransact_ExactlyOnceHappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	ctx := context.Background()
	const n = 5

	inTopic := "txn-in-" + uuid.NewString()[:8]
	outTopic := "txn-out-" + uuid.NewString()[:8]
	group := uniqueName("txn-happy-group")
	readerGroup := uniqueName("txn-happy-reader-group")
	txnID := uniqueName("txn-happy-txn-id")

	// ── Admin setup ──────────────────────────────────────────────────────────
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-happy-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, inTopic, outTopic))

	// ── Produce N records to in-topic ────────────────────────────────────────
	prod := kafka.NewProducer(adminCl)
	for i := 0; i < n; i++ {
		require.NoError(t, prod.Produce(ctx, kafka.Record{
			Topic: inTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		}))
	}
	require.NoError(t, prod.Close(ctx))

	// ── TransactConsumer: forward each record to out-topic ───────────────────
	tc, err := kafka.NewTransactConsumer(
		kafka.Config{Brokers: []string{broker}, ClientID: "txn-happy-tc"},
		txnID,
		group,
		[]string{inTopic},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	defer cancelRun()

	go func() {
		_ = tc.Run(runCtx, func(_ context.Context, rec kafka.Record) ([]kafka.Record, error) {
			return []kafka.Record{{
				Topic: outTopic,
				Key:   rec.Key,
				Value: []byte("out-" + string(rec.Value)),
			}}, nil
		})
	}()

	// ── Read-committed consumer reads out-topic ──────────────────────────────
	outCfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-happy-reader",
		GroupID:  readerGroup,
	}
	reader, err := kafka.NewConsumer(outCfg, []string{outTopic})
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		got     = make(map[string]int) // key → count
		gotDone = make(chan struct{})
		gotOnce sync.Once
	)

	readerCtx, cancelReader := context.WithTimeout(ctx, 60*time.Second)
	defer cancelReader()

	go func() {
		_ = reader.Run(readerCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			got[string(r.Key)]++
			if len(got) >= n {
				gotOnce.Do(func() { close(gotDone) })
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case <-gotDone:
	case <-readerCtx.Done():
		t.Fatal("timed out waiting for all output records")
	}

	// Allow a moment for commits to land before asserting no duplicates.
	time.Sleep(300 * time.Millisecond)
	cancelRun()
	cancelReader()
	_ = tc.Close(ctx)
	_ = reader.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, n, "expected %d unique keys in out-topic", n)
	for k, cnt := range got {
		assert.Equal(t, 1, cnt, "key %s appeared %d times (expected exactly 1)", k, cnt)
	}

	// ── Verify offsets committed: new session sees only NEW records ──────────
	// Produce 1 more record; a fresh consumer in the same group should see
	// exactly 1 output (the one just produced), not N+1.
	adminCl2, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-happy-admin2",
	})
	require.NoError(t, err)
	defer adminCl2.Close()

	prod2 := kafka.NewProducer(adminCl2)
	require.NoError(t, prod2.Produce(ctx, kafka.Record{
		Topic: inTopic,
		Key:   []byte("extra"),
		Value: []byte("extra-val"),
	}))
	require.NoError(t, prod2.Close(ctx))

	tc2, err := kafka.NewTransactConsumer(
		kafka.Config{Brokers: []string{broker}, ClientID: "txn-happy-tc2"},
		txnID+"-2", // new txn ID
		group,      // SAME group → picks up from committed offset
		[]string{inTopic},
	)
	require.NoError(t, err)

	var newCount atomic.Int32
	newDone := make(chan struct{})
	var newOnce sync.Once

	tc2Ctx, cancelTc2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTc2()

	go func() {
		_ = tc2.Run(tc2Ctx, func(_ context.Context, rec kafka.Record) ([]kafka.Record, error) {
			v := newCount.Add(1)
			if v >= 1 {
				newOnce.Do(func() { close(newDone) })
			}
			return []kafka.Record{{Topic: outTopic, Key: rec.Key, Value: rec.Value}}, nil
		})
	}()

	select {
	case <-newDone:
	case <-tc2Ctx.Done():
		t.Fatal("timed out waiting for the extra record in tc2")
	}
	time.Sleep(200 * time.Millisecond)
	cancelTc2()
	_ = tc2.Close(ctx)

	assert.Equal(t, int32(1), newCount.Load(),
		"tc2 (same group) should process exactly 1 new record; got %d", newCount.Load())
}

// TestTransact_ClientSideProduceFailureAborts verifies that a transaction
// whose OUTPUT record fails CLIENT-SIDE (never reaches the broker) is
// ABORTED and the input offsets are NOT committed, so the input record is
// redelivered. kgo fails a record larger than ProducerBatchMaxBytes
// (default 1000012, mirroring Kafka's max.message.bytes) at buffering time
// with kerr.MessageTooLarge — the producer ID stays healthy, so without an
// explicit per-record promise gate sess.End(TryCommit) would happily commit
// the input offsets while the output is silently lost.
func TestTransact_ClientSideProduceFailureAborts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	ctx := context.Background()

	inTopic := "txn-toolarge-in-" + uuid.NewString()[:8]
	outTopic := "txn-toolarge-out-" + uuid.NewString()[:8]

	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-toolarge-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, inTopic, outTopic))

	prod := kafka.NewProducer(adminCl)
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: inTopic,
		Key:   []byte("k"),
		Value: []byte("v"),
	}))
	require.NoError(t, prod.Close(ctx))

	// First attempt: produce a 2 MB output value — fails client-side
	// (exceeds the 1000012-byte default batch max) without touching the
	// producer ID. Later attempts: produce a small, valid output.
	var attempts atomic.Int32
	processFn := func(_ context.Context, rec kafka.Record) ([]kafka.Record, error) {
		n := attempts.Add(1)
		val := []byte("ok-" + string(rec.Value))
		if n == 1 {
			val = make([]byte, 2_000_000)
		}
		return []kafka.Record{{Topic: outTopic, Key: rec.Key, Value: val}}, nil
	}

	tc, err := kafka.NewTransactConsumer(
		kafka.Config{Brokers: []string{broker}, ClientID: "txn-toolarge-tc"},
		uniqueName("txn-toolarge-txn-id"),
		uniqueName("txn-toolarge-group"),
		[]string{inTopic},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRun()

	go func() {
		_ = tc.Run(runCtx, processFn)
	}()

	// Read-committed reader: must see exactly ONE output — the small value
	// from the successful redelivery attempt.
	reader, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-toolarge-reader",
		GroupID:  uniqueName("txn-toolarge-reader-group"),
	}, []string{outTopic})
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		outputs [][]byte
		gotOne  = make(chan struct{})
		gotOnce sync.Once
	)

	readerCtx, cancelReader := context.WithTimeout(ctx, 90*time.Second)
	defer cancelReader()

	go func() {
		_ = reader.Run(readerCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			outputs = append(outputs, r.Value)
			mu.Unlock()
			gotOnce.Do(func() { close(gotOne) })
			return nil
		})
	}()

	select {
	case <-gotOne:
	case <-readerCtx.Done():
		t.Fatal("timed out waiting for the redelivered output — the client-side produce failure was committed instead of aborted (input offsets advanced, output lost)")
	}

	// Allow late arrivals so duplicates would be detected.
	time.Sleep(500 * time.Millisecond)
	cancelRun()
	cancelReader()
	_ = tc.Close(ctx)
	_ = reader.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, outputs, 1, "exactly one committed output expected")
	assert.Equal(t, []byte("ok-v"), outputs[0], "output must come from the post-abort redelivery attempt")
	assert.GreaterOrEqual(t, attempts.Load(), int32(2),
		"input must have been redelivered after the aborted first attempt")
}

// TestTransact_AbortInvisibleAndRedelivered verifies the abort+redelivery
// contract:
//  1. A "poison" record causes fn to fail on first attempt → batch aborts →
//     all produced outputs of that batch are invisible to read-committed readers.
//  2. On redelivery fn succeeds → all inputs appear in out-topic exactly once.
func TestTransact_AbortInvisibleAndRedelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	ctx := context.Background()

	inTopic := "txn-abort-in-" + uuid.NewString()[:8]
	outTopic := "txn-abort-out-" + uuid.NewString()[:8]

	// ── Admin setup ──────────────────────────────────────────────────────────
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-abort-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, inTopic, outTopic))

	// Produce 3 records; one of them is the poison record (key="poison").
	const poisonKey = "poison"
	inputs := []kafka.Record{
		{Topic: inTopic, Key: []byte("a"), Value: []byte("va")},
		{Topic: inTopic, Key: []byte(poisonKey), Value: []byte("vp")},
		{Topic: inTopic, Key: []byte("b"), Value: []byte("vb")},
	}
	prod := kafka.NewProducer(adminCl)
	for _, r := range inputs {
		require.NoError(t, prod.Produce(ctx, r))
	}
	require.NoError(t, prod.Close(ctx))

	// failOnce: the poison key fails the FIRST time it is seen; succeeds after.
	var failMu sync.Mutex
	poisonFailed := false

	processFn := func(_ context.Context, rec kafka.Record) ([]kafka.Record, error) {
		if string(rec.Key) == poisonKey {
			failMu.Lock()
			already := poisonFailed
			if !already {
				poisonFailed = true
			}
			failMu.Unlock()
			if !already {
				return nil, fmt.Errorf("intentional first-pass failure for %q", poisonKey)
			}
		}
		return []kafka.Record{{
			Topic: outTopic,
			Key:   rec.Key,
			Value: rec.Value,
		}}, nil
	}

	tc, err := kafka.NewTransactConsumer(
		kafka.Config{Brokers: []string{broker}, ClientID: "txn-abort-tc"},
		uniqueName("txn-abort-txn-id"),
		uniqueName("txn-abort-group"),
		[]string{inTopic},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRun()

	go func() {
		_ = tc.Run(runCtx, processFn)
	}()

	// ── Read-committed consumer: collect all outputs ──────────────────────────
	outCfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-abort-reader",
		GroupID:  uniqueName("txn-abort-reader-group"),
	}
	reader, err := kafka.NewConsumer(outCfg, []string{outTopic})
	require.NoError(t, err)

	var (
		mu       sync.Mutex
		counts   = make(map[string]int)
		allDone  = make(chan struct{})
		doneOnce sync.Once
	)
	const wantUnique = 3

	readerCtx, cancelReader := context.WithTimeout(ctx, 90*time.Second)
	defer cancelReader()

	go func() {
		_ = reader.Run(readerCtx, func(_ context.Context, r kafka.Record) error {
			mu.Lock()
			counts[string(r.Key)]++
			if len(counts) >= wantUnique {
				doneOnce.Do(func() { close(allDone) })
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case <-allDone:
	case <-readerCtx.Done():
		t.Fatal("timed out waiting for all outputs after abort+redelivery")
	}

	// Allow a moment for any possible late arrivals so we can detect duplicates.
	time.Sleep(500 * time.Millisecond)
	cancelRun()
	cancelReader()
	_ = tc.Close(ctx)
	_ = reader.Close(ctx)

	mu.Lock()
	defer mu.Unlock()

	// Each input key must appear exactly once in the out-topic:
	//  - aborted-batch outputs were invisible to read-committed reader
	//  - redelivery produced them exactly once
	require.Len(t, counts, wantUnique, "expected %d unique keys; got %v", wantUnique, counts)
	for k, cnt := range counts {
		assert.Equal(t, 1, cnt,
			"key %q: expected exactly 1 output record, got %d (abort outputs must be invisible)", k, cnt)
	}

	// The poison key must have been retried (i.e., first attempt failed).
	failMu.Lock()
	wasFailed := poisonFailed
	failMu.Unlock()
	assert.True(t, wasFailed, "poison record must have triggered a first-pass failure to prove abort path")
}
