package retry_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/retry"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// setupIntegration creates the admin client, producer, escalator and ensures
// topics exist for the given base topic + policy. It returns a teardown
// function that closes the admin client.
func setupIntegration(
	t *testing.T,
	broker string,
	base string,
	pol retry.Policy,
) (adminCl *kgo.Client, prod *kafka.Producer, esc *retry.Escalator) {
	t.Helper()

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "retry-test-admin-" + uuid.NewString()[:8],
	})
	require.NoError(t, err)

	topics := make([]string, 0, 2+len(pol.Tiers))
	topics = append(topics, base, retry.DLTTopic(base))
	for i := range pol.Tiers {
		topics = append(topics, retry.TierTopic(base, i))
	}

	ctx := context.Background()
	require.NoError(t, kafka.EnsureTopics(ctx, cl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topics...))

	p := kafka.NewProducer(cl)
	e := retry.NewEscalator(p, pol)

	t.Cleanup(func() {
		_ = p.Close(context.Background())
		cl.Close()
	})

	return cl, p, e
}

// pollKGO reads up to maxRecords from topic within timeout using a raw kgo
// consumer (no group — reads from start).
func pollKGO(t *testing.T, broker, topic string, maxRecords int, timeout time.Duration) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ClientID("retry-test-poller"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var recs []*kgo.Record
	for len(recs) < maxRecords {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		recs = append(recs, fetches.Records()...)
	}
	return recs
}

// kgoHeaderVal returns a header value by key from a raw kgo record.
func kgoHeaderVal(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Test 1: redeliver after delay
// ---------------------------------------------------------------------------

// TestRetryConsumer_RedeliversAfterDelay:
//   - Policy: Tiers=[2s], FastAttempts=1.
//   - Main consumer processes the message; handler fails on first call → escalates to 2s tier.
//   - retry.Consumer waits until due-time, then redelivers; handler succeeds.
//   - Assert: second handler call happens ≥1.5s after first.
func TestRetryConsumer_RedeliversAfterDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pol := retry.Policy{Tiers: []time.Duration{2 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	base := "rt.base-" + suffix
	groupMain := "rt-main-" + suffix
	groupRetry := "rt-retry-" + suffix

	_, prod, esc := setupIntegration(t, broker, base, pol)

	// callTimes[key] = list of times the handler was called.
	var mu sync.Mutex
	callTimes := map[string][]time.Time{}
	successSeen := make(chan struct{})
	var successOnce sync.Once

	// Shared handler: fail first call for each key, succeed after.
	handler := func(_ context.Context, r kafka.Record) error {
		key := string(r.Key)
		mu.Lock()
		callTimes[key] = append(callTimes[key], time.Now())
		n := len(callTimes[key])
		mu.Unlock()

		if n == 1 {
			return errors.New("first-call failure")
		}
		// Second call: success
		successOnce.Do(func() { close(successSeen) })
		return nil
	}

	// Main consumer: on handler error, escalate to retry tier and commit.
	mainHandler := func(ctx context.Context, r kafka.Record) error {
		if err := handler(ctx, r); err != nil {
			if _, escErr := esc.Escalate(ctx, r.Topic, r, err); escErr != nil {
				return escErr
			}
			return nil // commit after escalation
		}
		return nil
	}

	mainCfg := kafka.Config{Brokers: []string{broker}, ClientID: "rt-main-" + suffix, GroupID: groupMain}
	mainConsumer, err := kafka.NewConsumer(mainCfg, []string{base})
	require.NoError(t, err)

	retryCfg := kafka.Config{Brokers: []string{broker}, ClientID: "rt-retry-" + suffix, GroupID: groupRetry}
	retryConsumer, err := retry.NewConsumer(retryCfg, groupRetry, []string{base}, handler, esc, pol)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start main consumer.
	go func() { _ = mainConsumer.Run(ctx, mainHandler) }()
	t.Cleanup(func() { _ = mainConsumer.Close(context.Background()) })

	// Start retry consumer.
	go func() { _ = retryConsumer.Run(ctx) }()
	t.Cleanup(func() { _ = retryConsumer.Close(context.Background()) })

	// Produce message K.
	err = prod.Produce(ctx, kafka.Record{
		Topic: base,
		Key:   []byte("K"),
		Value: []byte("hello"),
	})
	require.NoError(t, err)

	// Wait for success.
	select {
	case <-successSeen:
	case <-ctx.Done():
		t.Fatal("timed out waiting for retry handler to succeed")
	}

	cancel()

	mu.Lock()
	times := callTimes["K"]
	mu.Unlock()

	require.GreaterOrEqualf(t, len(times), 2, "handler must be called at least twice; got %d calls", len(times))

	gap := times[1].Sub(times[0])
	assert.GreaterOrEqualf(t, gap, 1500*time.Millisecond,
		"second handler call should be ≥1.5s after first (due-time honored); gap was %v", gap)
}

// ---------------------------------------------------------------------------
// Test 2: non-blocking — other traffic flows while a record waits
// ---------------------------------------------------------------------------

// TestRetryConsumer_DoesNotBlockOtherTraffic:
//   - Policy: Tiers=[5s].
//   - K1 fails on main consumer → escalated to 5s retry tier.
//   - K2 produced after K1; succeeds on main consumer.
//   - Assert K2 succeeds (main consumer not blocked by K1's retry wait on tier topic).
//   - Assert K1 retry succeeds ~5s later.
//
// The non-blocking property proven here: once K1 is escalated to the tier topic
// the main consumer commits K1's offset and moves on to K2 immediately. The 5s
// wait happens only on the retry.Consumer (tier topic), completely decoupled from
// the main topic flow.
func TestRetryConsumer_DoesNotBlockOtherTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pol := retry.Policy{Tiers: []time.Duration{5 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	base := "nb.base-" + suffix
	groupMain := "nb-main-" + suffix
	groupRetry := "nb-retry-" + suffix

	_, prod, esc := setupIntegration(t, broker, base, pol)

	var mu sync.Mutex
	callCounts := map[string]int{}
	k2Success := make(chan time.Time, 1)
	k1RetrySuccess := make(chan time.Time, 1)
	warmup := make(chan struct{}, 1)

	// Handler: K1 always fails on first call (escalated by mainHandler);
	// succeeds on retry. K2 always succeeds. (WARMUP is handled in mainHandler,
	// so only the MAIN consumer — the one that must process K2 — signals readiness.)
	handler := func(_ context.Context, r kafka.Record) error {
		key := string(r.Key)
		mu.Lock()
		callCounts[key]++
		n := callCounts[key]
		mu.Unlock()

		switch key {
		case "K1":
			if n == 1 {
				return errors.New("K1 transient failure")
			}
			// K1 retry success
			select {
			case k1RetrySuccess <- time.Now():
			default:
			}
			return nil
		case "K2":
			select {
			case k2Success <- time.Now():
			default:
			}
			return nil
		}
		return nil
	}

	mainHandler := func(ctx context.Context, r kafka.Record) error {
		if string(r.Key) == "WARMUP" {
			// Only the MAIN consumer signals readiness — it is the one that must
			// process K2 without being blocked by K1's retry tier.
			select {
			case warmup <- struct{}{}:
			default:
			}
			return nil
		}
		if err := handler(ctx, r); err != nil {
			if _, escErr := esc.Escalate(ctx, r.Topic, r, err); escErr != nil {
				return escErr
			}
			return nil
		}
		return nil
	}

	mainCfg := kafka.Config{Brokers: []string{broker}, ClientID: "nb-main-" + suffix, GroupID: groupMain}
	mainConsumer, err := kafka.NewConsumer(mainCfg, []string{base})
	require.NoError(t, err)

	retryCfg := kafka.Config{Brokers: []string{broker}, ClientID: "nb-retry-" + suffix, GroupID: groupRetry}
	retryConsumer, err := retry.NewConsumer(retryCfg, groupRetry, []string{base}, handler, esc, pol)
	require.NoError(t, err)

	// 60s: comfortably covers warmup group-join (slow under -p 1) + K1's 5s
	// retry tier within one deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	go func() { _ = mainConsumer.Run(ctx, mainHandler) }()
	t.Cleanup(func() { _ = mainConsumer.Close(context.Background()) })

	go func() { _ = retryConsumer.Run(ctx) }()
	t.Cleanup(func() { _ = retryConsumer.Close(context.Background()) })

	// Warm up: produce a throwaway record and wait until the main consumer has
	// processed it, proving its group is joined and polling. Group-join
	// (rebalance) can take several seconds when the Docker host is saturated
	// under -p 1; doing it BEFORE the timed window below keeps the K1/K2
	// non-blocking assertion robust (it measures retry behaviour, not cold-start
	// latency). Generous bound — this is the slow part, not the thing under test.
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: base, Key: []byte("WARMUP"), Value: []byte("warmup"),
	}))
	select {
	case <-warmup:
	case <-time.After(25 * time.Second):
		t.Fatal("main consumer did not join its group / process the warmup record within 25s")
	}

	start := time.Now()

	// Produce K1 (will fail + be escalated to 5s tier).
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: base, Key: []byte("K1"), Value: []byte("v1"),
	}))

	// Produce K2 (should succeed quickly on main consumer).
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: base, Key: []byte("K2"), Value: []byte("v2"),
	}))

	// K2 must succeed well before K1's retry fires (which is 5s away).
	// We give 4s for K2 — generous for container + group-join latency, but still
	// proves K2 is NOT blocked by K1's 5s wait on the retry tier topic.
	var k2Time time.Time
	select {
	case k2Time = <-k2Success:
	case <-time.After(4 * time.Second):
		t.Fatal("K2 was not processed within 4s — main consumer may be blocked by K1's retry")
	}
	// K2 must arrive strictly before K1's retry (5s from start), so the gap
	// proves K2 is not waiting for K1's due-time.
	assert.Less(t, k2Time.Sub(start), 5*time.Second,
		"K2 should have been processed before K1's 5s retry fires")

	// K1 retry should succeed after ~5s.
	select {
	case <-k1RetrySuccess:
	case <-ctx.Done():
		t.Fatal("K1 retry was never processed within 30s")
	}
}

// ---------------------------------------------------------------------------
// Test 3: poison record walks all tiers to DLT
// ---------------------------------------------------------------------------

// TestRetryConsumer_PoisonToDLT:
//   - Policy: Tiers=[1s, 2s] — two distinct tiers to avoid duplicate topic names.
//   - Handler always fails.
//   - Record walks: base → tier-0 (1s) → tier-1 (2s) → DLT.
//   - Assert: record arrives on DLT with retry-attempt header == "2".
func TestRetryConsumer_PoisonToDLT(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pol := retry.Policy{Tiers: []time.Duration{1 * time.Second, 2 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	base := "dlt.base-" + suffix
	groupMain := "dlt-main-" + suffix
	groupRetry := "dlt-retry-" + suffix

	_, prod, esc := setupIntegration(t, broker, base, pol)

	// Handler always fails.
	handler := func(_ context.Context, _ kafka.Record) error {
		return errors.New("permanent failure")
	}

	mainHandler := func(ctx context.Context, r kafka.Record) error {
		if _, escErr := esc.Escalate(ctx, r.Topic, r, errors.New("permanent failure")); escErr != nil {
			return escErr
		}
		return nil
	}

	mainCfg := kafka.Config{Brokers: []string{broker}, ClientID: "dlt-main-" + suffix, GroupID: groupMain}
	mainConsumer, err := kafka.NewConsumer(mainCfg, []string{base})
	require.NoError(t, err)

	retryCfg := kafka.Config{Brokers: []string{broker}, ClientID: "dlt-retry-" + suffix, GroupID: groupRetry}
	retryConsumer, err := retry.NewConsumer(retryCfg, groupRetry, []string{base}, handler, esc, pol)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() { _ = mainConsumer.Run(ctx, mainHandler) }()
	t.Cleanup(func() { _ = mainConsumer.Close(context.Background()) })

	go func() { _ = retryConsumer.Run(ctx) }()
	t.Cleanup(func() { _ = retryConsumer.Close(context.Background()) })

	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: base,
		Key:   []byte("K-poison"),
		Value: []byte("will-fail"),
	}))

	dltTopic := retry.DLTTopic(base)

	// Poll the DLT for up to 20s.
	dltRecords := pollKGO(t, broker, dltTopic, 1, 20*time.Second)

	cancel()

	require.Len(t, dltRecords, 1, "expected exactly 1 record on DLT")
	dlt := dltRecords[0]
	assert.Equal(t, []byte("K-poison"), dlt.Key)

	attempt := kgoHeaderVal(dlt, retry.HeaderAttempt)
	fmt.Printf("DLT record attempt header: %q\n", attempt)
	// After 2 tiers: attempt should be "2" (two escalations done).
	assert.Equal(t, "2", attempt, "DLT record should have retry-attempt=2 after two-tier walk")
}

// ---------------------------------------------------------------------------
// Test 4: malformed record (no retry headers) goes to DLT directly
// ---------------------------------------------------------------------------

// TestRetryConsumer_MalformedRecordToDLT verifies that a record without retry
// headers that ends up on a retry topic is escalated directly to the DLT
// rather than crashing the consumer.
func TestRetryConsumer_MalformedRecordToDLT(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pol := retry.Policy{Tiers: []time.Duration{1 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	base := "malf.base-" + suffix
	groupRetry := "malf-retry-" + suffix

	adminCl, prod, esc := setupIntegration(t, broker, base, pol)
	_ = adminCl

	handler := func(_ context.Context, _ kafka.Record) error { return nil }

	retryCfg := kafka.Config{Brokers: []string{broker}, ClientID: "malf-retry-" + suffix, GroupID: groupRetry}
	retryConsumer, err := retry.NewConsumer(retryCfg, groupRetry, []string{base}, handler, esc, pol)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() { _ = retryConsumer.Run(ctx) }()
	t.Cleanup(func() { _ = retryConsumer.Close(context.Background()) })

	// Produce a record with NO retry headers directly to the tier topic.
	tierTopic := retry.TierTopic(base, 0)
	require.NoError(t, prod.Produce(ctx, kafka.Record{
		Topic: tierTopic,
		Key:   []byte("bad"),
		Value: []byte("no-headers"),
		// No retry headers.
	}))

	dltTopic := retry.DLTTopic(base)
	dltRecords := pollKGO(t, broker, dltTopic, 1, 15*time.Second)
	cancel()

	require.Len(t, dltRecords, 1, "malformed record should be moved to DLT")
	assert.Equal(t, []byte("bad"), dltRecords[0].Key)
}

// Ensure the atomic import is used.
var _ = atomic.Int32{}

// ---------------------------------------------------------------------------
// Test 5: Close with a pending hold — no blocked goroutines
// ---------------------------------------------------------------------------

// TestRetryConsumer_CloseWithPendingHold proves that closing a retry consumer
// while a hold-timer is pending (Tiers=[30s] — very long, never fires) does
// not leak any goroutines and returns promptly.
//
// This is the integration companion to the unit test
// TestClose_StopsPendingTimers_NoBlockedGoroutines — it exercises the full
// path through NewConsumer + a real Kafka broker.
func TestRetryConsumer_CloseWithPendingHold(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	// Ignore long-lived infrastructure goroutines that are not under test:
	//   - testcontainers Reaper keeps a connection-monitoring goroutine for the
	//     lifetime of the test binary.
	//   - kgo Client goroutines from setupIntegration admin clients (closed via
	//     t.Cleanup but may still be draining when VerifyNone runs).
	defer goleak.VerifyNone(
		t,
		goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
	)

	broker, _ := kafkatest.NewRedpanda(t)

	// Long tier (30s) so the hold timer never fires during the test.
	pol := retry.Policy{Tiers: []time.Duration{30 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	base := "cls.base-" + suffix
	groupRetry := "cls-retry-" + suffix

	adminCl, prod, esc := setupIntegration(t, broker, base, pol)
	_ = adminCl

	// Handler always returns an error so the escalator writes to the tier topic
	// and the retry consumer's processPartition holds the record (not yet due).
	handler := func(_ context.Context, _ kafka.Record) error {
		return errors.New("permanent failure — hold pending")
	}

	retryCfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "cls-retry-" + suffix,
		GroupID:  groupRetry,
	}
	retryConsumer, err := retry.NewConsumer(retryCfg, groupRetry, []string{base}, handler, esc, pol)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Produce a record with retry headers pointing 30s into the future so
	// the retry consumer holds it immediately on first poll.
	tierTopic := retry.TierTopic(base, 0)
	dueAt := time.Now().Add(30 * time.Second)
	rec := kafka.Record{
		Topic: tierTopic,
		Key:   []byte("hold-me"),
		Value: []byte("v"),
		Headers: map[string]string{
			retry.HeaderAttempt:   "1",
			retry.HeaderOrigTopic: base,
			retry.HeaderDueAt:     strconv.FormatInt(dueAt.UnixMilli(), 10),
		},
	}
	require.NoError(t, prod.Produce(ctx, rec))

	// Start retry consumer; give it time to fetch and hold the record.
	runDone := make(chan error, 1)
	go func() { runDone <- retryConsumer.Run(ctx) }()

	// Wait long enough for the consumer to poll and hold the record.
	time.Sleep(3 * time.Second)

	// Close must return promptly — timer must be stopped, no goroutine blocked.
	closeDone := make(chan error, 1)
	go func() { closeDone <- retryConsumer.Close(context.Background()) }()

	select {
	case err := <-closeDone:
		require.NoError(t, err, "Close must not return an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked for >5s — timer goroutine or Run loop leaked")
	}

	// Run loop should also exit quickly after client is closed.
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s after Close")
	}
}

// ---------------------------------------------------------------------------
// Test 6: escalation failure must not lose the record
// ---------------------------------------------------------------------------

// flakyProducer fails the first failN Produce calls, then delegates.
type flakyProducer struct {
	mu    sync.Mutex
	fails int
	failN int
	real  *kafka.Producer
}

func (f *flakyProducer) Produce(ctx context.Context, rec kafka.Record) error {
	f.mu.Lock()
	if f.fails < f.failN {
		f.fails++
		f.mu.Unlock()
		return errors.New("flaky: injected produce failure")
	}
	f.mu.Unlock()
	return f.real.Produce(ctx, rec)
}

// TestRetryConsumer_EscalateFailureNoLoss proves that a record whose
// escalation produce fails is retried — not silently skipped while later
// records commit past it (which would lose it permanently).
//
//   - r1 ("bad"): handler always fails → escalate → DLT produce fails twice
//     (injected), succeeds on the 3rd attempt.
//   - r2 ("good"): handler succeeds.
//
// Broken semantics: r1's failed escalation skips the record, r2 commits past
// it, r1 is never re-fetched → DLT never receives it.
func TestRetryConsumer_EscalateFailureNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	// Registered FIRST so it runs LAST (t.Cleanup is LIFO) — i.e. after the
	// pollKGO and setupIntegration cleanups have closed their kgo clients.
	// Same infrastructure-goroutine ignores as TestRetryConsumer_CloseWithPendingHold.
	t.Cleanup(func() {
		goleak.VerifyNone(
			t,
			goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
			goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
			goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
		)
	})

	broker, _ := kafkatest.NewRedpanda(t)
	base := "esc-fail-" + uuid.NewString()[:8]
	pol := retry.Policy{Tiers: []time.Duration{time.Second}, FastAttempts: 1}

	_, prod, _ := setupIntegration(t, broker, base, pol)

	flaky := &flakyProducer{failN: 2, real: prod}
	esc := retry.NewEscalator(flaky, pol)

	tierTopic := retry.TierTopic(base, 0)
	ctx := context.Background()

	// Two already-due records on the single tier partition: bad first, good second.
	for _, v := range []string{"bad", "good"} {
		rec := kafka.Record{Topic: tierTopic, Key: []byte(v), Value: []byte(v)}
		retry.SetRetryHeaders(&rec, 1, base, time.Now().Add(-time.Second), errors.New("seed"))
		require.NoError(t, prod.Produce(ctx, rec))
	}

	var goodProcessed atomic.Bool
	handler := func(_ context.Context, r kafka.Record) error {
		if string(r.Value) == "bad" {
			return errors.New("handler: permanent failure")
		}
		goodProcessed.Store(true)
		return nil
	}

	cons, err := retry.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "esc-fail-consumer",
	}, "esc-fail-group", []string{base}, handler, esc, pol)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = cons.Run(runCtx)
	}()

	// The bad record must reach the DLT despite two injected escalation
	// failures — proves escalation is retried, not lost.
	dltRecs := pollKGO(t, broker, retry.DLTTopic(base), 1, 60*time.Second)
	require.Len(t, dltRecs, 1, "bad record lost: escalation failure skipped it and committed past")
	assert.Equal(t, "bad", string(dltRecs[0].Value))

	require.Eventually(t, goodProcessed.Load, 30*time.Second, 100*time.Millisecond,
		"good record must still be processed")

	cancel()
	<-runDone
	require.NoError(t, cons.Close(ctx))
}
