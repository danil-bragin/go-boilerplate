package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedDLT produces a record onto the DLT topic with the given headers.
func seedDLT(t *testing.T, broker, dlt string, key, value string, headers map[string]string) {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "redrive-test-seeder"})
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	}()
	require.NoError(t, p.Produce(context.Background(), kafka.Record{
		Topic:   dlt,
		Key:     []byte(key),
		Value:   []byte(value),
		Headers: headers,
	}))
}

// collectRecords consumes up to n records from topic (fresh group, from start).
func collectRecords(t *testing.T, broker, topic string, n int, timeout time.Duration) []kafka.Record {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "redrive-test-collector",
		GroupID:  "redrive-test-collector-" + uuid.New().String(),
	}
	consumer, err := kafka.NewConsumer(cfg, []string{topic})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	out := make(chan kafka.Record, n)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			select {
			case out <- r:
			default:
			}
			return nil
		})
	}()

	var got []kafka.Record
	for len(got) < n {
		select {
		case r := <-out:
			got = append(got, r)
		case <-ctx.Done():
			t.Fatalf("collected %d/%d records from %s within %v", len(got), n, topic, timeout)
		}
	}
	return got
}

func ensure(t *testing.T, broker string, topics ...string) {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: "redrive-test-admin"})
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, kafka.EnsureTopics(context.Background(), cl,
		kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topics...))
}

// TestRedrive_RepublishesToOriginalTopic seeds DLT records carrying both
// orig-topic header conventions (WithRetry's x-original-topic and tiered
// retry's retry-orig-topic) and asserts redrive republishes each to its
// original topic with retry/diagnostic headers stripped and the original
// message-id + custom headers preserved.
func TestRedrive_RepublishesToOriginalTopic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	const orig = "orders.commands"
	const dlt = "orders.commands.DLT"
	ensure(t, broker, orig, dlt)

	msgID1 := uuid.New().String()
	seedDLT(t, broker, dlt, "k1", "v1", map[string]string{
		"x-original-topic": orig,
		"x-error":          "boom",
		"x-attempts":       "3",
		"message-id":       msgID1,
		"event-type":       "orders.CreateOrderCommand.v1",
		"correlation-id":   "corr-1",
	})
	msgID2 := uuid.New().String()
	seedDLT(t, broker, dlt, "k2", "v2", map[string]string{
		"retry-orig-topic": orig,
		"retry-attempt":    "4",
		"retry-due-at":     "12345",
		"retry-last-error": "kapow",
		"message-id":       msgID2,
	})

	stats, err := Run(context.Background(), Config{
		Brokers: []string{broker},
		DLT:     dlt,
		Group:   "redrive-test-" + uuid.New().String(),
		Out:     io.Discard,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Republished)

	recs := collectRecords(t, broker, orig, 2, 30*time.Second)
	byKey := map[string]kafka.Record{}
	for _, r := range recs {
		byKey[string(r.Key)] = r
	}

	r1, ok := byKey["k1"]
	require.True(t, ok, "record k1 must be republished")
	assert.Equal(t, "v1", string(r1.Value))
	assert.Equal(t, msgID1, r1.Headers["message-id"], "message-id preserved (inbox dedup intact)")
	assert.Equal(t, "corr-1", r1.Headers["correlation-id"], "custom headers preserved")
	assert.Equal(t, "orders.CreateOrderCommand.v1", r1.Headers["event-type"])
	for _, h := range []string{"x-original-topic", "x-error", "x-attempts"} {
		assert.NotContains(t, r1.Headers, h, "diagnostic header %s must be stripped", h)
	}

	r2, ok := byKey["k2"]
	require.True(t, ok, "record k2 must be republished")
	assert.Equal(t, "v2", string(r2.Value))
	assert.Equal(t, msgID2, r2.Headers["message-id"])
	for _, h := range []string{"retry-orig-topic", "retry-attempt", "retry-due-at", "retry-last-error"} {
		assert.NotContains(t, r2.Headers, h, "retry header %s must be stripped", h)
	}
}

// TestRedrive_FreshIDsAndDryRun covers the two replay modes:
//   - --dry-run lists without republishing or committing;
//   - --fresh-ids replaces message-id so inbox dedup does NOT collapse the
//     replay (projection rebuild mode).
func TestRedrive_FreshIDsAndDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	const orig = "payments.events"
	const dlt = "payments.events.DLT"
	ensure(t, broker, orig, dlt)

	msgID := uuid.New().String()
	seedDLT(t, broker, dlt, "k1", "v1", map[string]string{
		"x-original-topic": orig,
		"message-id":       msgID,
	})

	// Dry run: nothing republished, nothing committed.
	stats, err := Run(context.Background(), Config{
		Brokers: []string{broker},
		DLT:     dlt,
		DryRun:  true,
		Group:   "redrive-dry-" + uuid.New().String(),
		Out:     io.Discard,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Read)
	assert.Zero(t, stats.Republished, "dry-run must not republish")

	// Fresh-ids replay: republished with a NEW message-id.
	stats, err = Run(context.Background(), Config{
		Brokers:  []string{broker},
		DLT:      dlt,
		FreshIDs: true,
		Group:    "redrive-fresh-" + uuid.New().String(),
		Out:      io.Discard,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Republished)

	recs := collectRecords(t, broker, orig, 1, 30*time.Second)
	assert.Equal(t, "v1", string(recs[0].Value))
	assert.NotEmpty(t, recs[0].Headers["message-id"])
	assert.NotEqual(t, msgID, recs[0].Headers["message-id"],
		"--fresh-ids must mint a new message-id (bypasses inbox dedup)")
}

// TestRedrive_MissingOrigTopicFails: a DLT record without either orig-topic
// header convention aborts the run with an error (nothing committed).
func TestRedrive_MissingOrigTopicFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	const dlt = "broken.DLT"
	ensure(t, broker, dlt)

	seedDLT(t, broker, dlt, "k1", "v1", map[string]string{"message-id": "m1"})

	_, err := Run(context.Background(), Config{
		Brokers: []string{broker},
		DLT:     dlt,
		Group:   "redrive-broken-" + uuid.New().String(),
		Out:     io.Discard,
	})
	require.Error(t, err, "record without an orig-topic header must abort the run")
}

// TestRedrive_WarnsOnMissingMessageID: records WITHOUT a message-id header
// are NOT deduped by consumers after redrive — their fallback inbox identity
// is topic:partition:offset, which changes on republish. The run must warn
// per record and count them in the summary so the operator knows the side
// effects will run again.
func TestRedrive_WarnsOnMissingMessageID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	const orig = "orders.events"
	const dlt = "orders.events.DLT"
	ensure(t, broker, orig, dlt)

	// One record with a message-id (dedup-safe), one without (not dedup-safe).
	seedDLT(t, broker, dlt, "k-with", "v1", map[string]string{
		"x-original-topic": orig,
		"message-id":       uuid.New().String(),
	})
	seedDLT(t, broker, dlt, "k-without", "v2", map[string]string{
		"x-original-topic": orig,
	})

	var out strings.Builder
	stats, err := Run(context.Background(), Config{
		Brokers: []string{broker},
		DLT:     dlt,
		Group:   "redrive-noid-" + uuid.New().String(),
		Out:     &out,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Republished)
	assert.Equal(t, 1, stats.MissingMessageID,
		"exactly one record lacked a message-id header")

	assert.Contains(t, out.String(), "no message-id header",
		"each record without a message-id must be flagged (inbox dedup will NOT collapse the replay)")
	assert.Contains(t, out.String(), "1 record(s) without message-id",
		"the summary must total the non-dedupable records")
}
