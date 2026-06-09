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
	broker, _ := kafkatest.NewRedpanda(t)

	ctx := context.Background()
	const n = 5

	inTopic := "txn-in-" + uuid.NewString()[:8]
	outTopic := "txn-out-" + uuid.NewString()[:8]

	// ── Admin setup ──────────────────────────────────────────────────────────
	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-happy-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, 1, 1, inTopic, outTopic))

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
		"txn-happy-txn-id",
		"txn-happy-group",
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
		GroupID:  "txn-happy-reader-group",
	}
	reader, err := kafka.NewConsumer(outCfg, outTopic)
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
		"txn-happy-txn-id-2", // new txn ID
		"txn-happy-group",    // SAME group → picks up from committed offset
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

// TestTransact_AbortInvisibleAndRedelivered verifies the abort+redelivery
// contract:
//  1. A "poison" record causes fn to fail on first attempt → batch aborts →
//     all produced outputs of that batch are invisible to read-committed readers.
//  2. On redelivery fn succeeds → all inputs appear in out-topic exactly once.
func TestTransact_AbortInvisibleAndRedelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.NewRedpanda(t)

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

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl, 1, 1, inTopic, outTopic))

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
		"txn-abort-txn-id",
		"txn-abort-group",
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
		GroupID:  "txn-abort-reader-group",
	}
	reader, err := kafka.NewConsumer(outCfg, outTopic)
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
