package kafka_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// pollDLT reads up to maxRecords from topic using a raw kgo client.  It
// returns after maxRecords are collected or the context expires (whichever
// comes first).
func pollDLT(t *testing.T, broker, topic string, maxRecords int, timeout time.Duration) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ClientID("dlq-test-reader"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer cl.Close()

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

// headerVal returns the value of a header by key from a kgo.Record.
func headerVal(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// TestWithRetry_RoutesToDLTAfterMaxAttempts verifies that a handler that
// always errors is called exactly MaxAttempts times and that the record is
// then produced to the DLT with the correct headers/key/value.  The wrapped
// handler must return nil (commit).
func TestWithRetry_RoutesToDLTAfterMaxAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.NewRedpanda(t)
	ctx := context.Background()

	const (
		topic    = "work"
		dltTopic = "work.DLT"
	)

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "dlq-test-admin",
	})
	require.NoError(t, err)
	defer cl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, cl, 1, 1, topic, dltTopic))

	producer := kafka.NewProducer(cl)

	var callCount atomic.Int32
	handler := func(_ context.Context, _ kafka.Record) error {
		callCount.Add(1)
		return errors.New("permanent failure")
	}

	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: 2,
		Producer:    producer,
		Backoff:     time.Millisecond,
	})

	rec := kafka.Record{
		Topic:   topic,
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: map[string]string{"orig": "1"},
	}

	err = wrapped(ctx, rec)
	require.NoError(t, err, "WithRetry must return nil after routing to DLT")

	assert.Equal(t, int32(2), callCount.Load(), "handler should be called exactly MaxAttempts times")

	// Consume the DLT and verify the record.
	dltRecords := pollDLT(t, broker, dltTopic, 1, 30*time.Second)
	require.Len(t, dltRecords, 1, "exactly one record must appear on the DLT")

	dlt := dltRecords[0]
	assert.Equal(t, []byte("k"), dlt.Key, "DLT record key must match original")
	assert.Equal(t, []byte("v"), dlt.Value, "DLT record value must match original")
	assert.NotEmpty(t, headerVal(dlt, "x-error"), "x-error header must be set")
	assert.Equal(t, "2", headerVal(dlt, "x-attempts"), "x-attempts must equal MaxAttempts")
	assert.Equal(t, topic, headerVal(dlt, "x-original-topic"), "x-original-topic must equal source topic")
	assert.Equal(t, "1", headerVal(dlt, "orig"), "original headers must be preserved")
}

// TestWithRetry_SucceedsWithinAttempts verifies that when the handler
// succeeds before exhausting MaxAttempts the wrapper returns nil and
// nothing is produced to the DLT.
func TestWithRetry_SucceedsWithinAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.NewRedpanda(t)
	ctx := context.Background()

	const (
		topic    = "work2"
		dltTopic = "work2.DLT"
	)

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "dlq-test-admin2",
	})
	require.NoError(t, err)
	defer cl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, cl, 1, 1, topic, dltTopic))

	producer := kafka.NewProducer(cl)

	var callCount atomic.Int32
	handler := func(_ context.Context, _ kafka.Record) error {
		n := callCount.Add(1)
		if n == 1 {
			return errors.New("transient error")
		}
		return nil
	}

	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: 3,
		Producer:    producer,
		Backoff:     time.Millisecond,
	})

	err = wrapped(ctx, kafka.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")})
	require.NoError(t, err, "WithRetry must return nil when handler succeeds within attempts")

	assert.Equal(t, int32(2), callCount.Load(), "handler should be called 2 times (1 fail + 1 success)")

	// DLT must be empty.
	dltRecords := pollDLT(t, broker, dltTopic, 1, 3*time.Second)
	assert.Empty(t, dltRecords, "no records should appear on the DLT when handler succeeds")
}

// TestWithRetry_CtxCancelStopsRetry verifies that a context cancellation
// during the backoff sleep causes the wrapper to return ctx.Err() without
// writing to the DLT.
func TestWithRetry_CtxCancelStopsRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.NewRedpanda(t)

	const (
		topic    = "work3"
		dltTopic = "work3.DLT"
	)

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "dlq-test-admin3",
	})
	require.NoError(t, err)
	defer cl.Close()

	setupCtx := context.Background()
	require.NoError(t, kafka.EnsureTopics(setupCtx, cl, 1, 1, topic, dltTopic))

	producer := kafka.NewProducer(cl)

	handler := func(_ context.Context, _ kafka.Record) error {
		return errors.New("always fails")
	}

	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: 5,
		Producer:    producer,
		Backoff:     50 * time.Millisecond,
	})

	// Cancel the context after ~10 ms so it fires during the first backoff sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = wrapped(ctx, kafka.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")})
	assert.ErrorIs(t, err, context.DeadlineExceeded, "WithRetry must propagate ctx.Err() on cancellation")

	// DLT must be empty.
	dltRecords := pollDLT(t, broker, dltTopic, 1, 3*time.Second)
	assert.Empty(t, dltRecords, "no records should appear on the DLT when ctx is cancelled")
}
