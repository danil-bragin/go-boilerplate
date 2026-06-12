package servicekit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// batchConsumerCfg builds a minimal servicekit.Config wired to the given broker
// and DSN, with a fast retry policy so the DLT-parity test does not wait long.
func batchConsumerCfg(t *testing.T, broker, dsn string) servicekit.Config {
	t.Helper()
	cfg := servicekit.Config{AdminAddr: "127.0.0.1:0"}
	cfg.EnsureTopics = true
	cfg.PG.DSN = config.Secret(dsn)
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	// No inbox-cleanup loop; we create the inbox table by hand below.
	cfg.InboxCleanupInterval = 0
	// Keep the in-process retry window short so poison reaches the DLT fast.
	cfg.ConsumerRetryMaxAttempts = 3
	cfg.ConsumerRetryBackoff = 50 * time.Millisecond
	return cfg
}

// createInboxTable installs the minimal inbox schema BatchHandler/ProcessOnce
// need, matching the consume package's test harness.
func createInboxTable(t *testing.T, svc *servicekit.Service) {
	t.Helper()
	_, err := svc.Pool().Writer().Exec(context.Background(), `
		create table if not exists inbox (
			consumer     text not null,
			message_id   text not null,
			processed_at timestamptz not null default now(),
			primary key (consumer, message_id)
		)`)
	require.NoError(t, err)
}

// orderRecord builds a Confluent-unframed (raw proto) OrderCreated kafka.Record
// for the given topic/order id with the standard event-type + message-id headers.
func orderRecord(t *testing.T, topic, orderID, msgID string) kafka.Record {
	t.Helper()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: orderID, CustomerId: "c1", AmountCents: 100, Currency: "USD",
	})
	require.NoError(t, err)
	return kafka.Record{
		Topic:   topic,
		Key:     []byte(orderID),
		Value:   payload,
		Headers: map[string]string{"event-type": "orders.OrderCreated.v1", "message-id": msgID},
	}
}

// produceAll writes the given records to the broker via a one-off producer.
func produceAll(t *testing.T, broker, clientID string, recs ...kafka.Record) {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{Brokers: []string{broker}, ClientID: clientID})
	require.NoError(t, err)
	prod := kafka.NewProducer(cl)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = prod.Close(c)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, r := range recs {
		require.NoError(t, prod.Produce(ctx, r))
	}
}

// pollTopic reads up to maxRecords from topic using a raw kgo client, returning
// when maxRecords are collected or the timeout expires (whichever comes first).
func pollTopic(t *testing.T, broker, topic, clientID string, maxRecords int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ClientID(clientID),
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

// TestAddBatchConsumer_PoisonEscalatesToDLT proves failure-contract parity with
// AddConsumer: a batch [good, poison, good2] whose per-record fallback returns a
// PERMANENT error for the poison payload escalates the poison record to
// <topic>.DLT (via the kafka.WithRetry wrap AddBatchConsumer bakes in), while
// good + good2 are applied. Without the wrap the poison would hot-loop the
// partition and good2 would never apply.
func TestAddBatchConsumer_PoisonEscalatesToDLT(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)
	suffix := uuid.NewString()[:8]
	topic := "svckit.batch-dlt-" + suffix
	groupID := "svckit-batch-dlt-grp-" + suffix
	dltTopic := topic + ".DLT"

	ctx := context.Background()
	svc, err := servicekit.New(ctx, batchConsumerCfg(t, broker, dsn), nil, "")
	require.NoError(t, err)
	createInboxTable(t, svc)

	// Track which order ids were durably applied. "good" can run twice (the
	// doomed batch tx which rolls back, then the fallback), so dedup by id —
	// the durable inbox guarantee is exactly-once, which we assert separately.
	var mu sync.Mutex
	appliedIDs := map[string]bool{}
	c := consume.New(svc.Pool(), groupID)
	handlers := []consume.Handler{
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, evt *ordersv1.OrderCreated) error {
			if evt.GetOrderId() == "poison" {
				// PERMANENT → WithRetry short-circuits straight to the DLT.
				return apperr.Newf(apperr.CodeMalformedPayload, "poison pill")
			}
			mu.Lock()
			appliedIDs[evt.GetOrderId()] = true
			mu.Unlock()
			return nil
		}),
	}

	err = svc.AddBatchConsumer(
		ctx, groupID, []string{topic},
		// fallback: per-record handler the batch falls back to.
		c.Handler(handlers...),
		// build: construct the batch handler from the WithRetry-wrapped fallback.
		func(fb kafka.HandlerFunc) kafka.BatchHandlerFunc {
			return c.BatchHandler(fb, handlers...)
		},
	)
	require.NoError(t, err)

	require.NoError(t, svc.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Stop(stopCtx)
	})

	produceAll(
		t, broker, "svckit-batch-prod-"+suffix,
		orderRecord(t, topic, "good", "m-good"),
		orderRecord(t, topic, "poison", "m-poison"),
		orderRecord(t, topic, "good2", "m-good2"),
	)

	// The poison record must land on the DLT.
	dltRecs := pollTopic(t, broker, dltTopic, "svckit-batch-dlt-reader-"+suffix, 1, 30*time.Second)
	require.Len(t, dltRecs, 1, "poison record must be escalated to the DLT")
	assert.Equal(t, []byte("poison"), dltRecs[0].Key, "DLT record is the poison record")

	// good + good2 must both be applied (batch advanced past the poison).
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return appliedIDs["good"] && appliedIDs["good2"]
	}, 30*time.Second, 200*time.Millisecond, "good + good2 must both be applied")

	// Durable exactly-once: exactly the two good rows persist in the inbox
	// (poison rolled back / went to the DLT, never committed an inbox row).
	var n int
	require.NoError(t, svc.Pool().Reader().QueryRow(context.Background(),
		`select count(*) from inbox where consumer = $1`, groupID).Scan(&n))
	assert.Equal(t, 2, n, "exactly good + good2 persist durably; poison never commits an inbox row")
}

// TestAddBatchConsumer_TransientResolvesByRetryNotDLT proves the transient case:
// a fallback handler that fails the first N-1 attempts with a TRANSIENT error
// then succeeds is resolved by kafka.WithRetry's in-process retry — the record
// is ultimately applied and NOTHING is produced to the DLT.
func TestAddBatchConsumer_TransientResolvesByRetryNotDLT(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)
	suffix := uuid.NewString()[:8]
	topic := "svckit.batch-transient-" + suffix
	groupID := "svckit-batch-transient-grp-" + suffix
	dltTopic := topic + ".DLT"

	ctx := context.Background()
	cfg := batchConsumerCfg(t, broker, dsn)
	cfg.ConsumerRetryMaxAttempts = 5 // plenty of attempts for the transient failures
	svc, err := servicekit.New(ctx, cfg, nil, "")
	require.NoError(t, err)
	createInboxTable(t, svc)

	// Fail the first 2 attempts with a TRANSIENT (non-permanent) error, then
	// succeed. The batch tx fails first (count=1), then the fallback retries
	// in-process via WithRetry until success.
	var attempts atomic.Int32
	applied := make(chan struct{}, 1)
	c := consume.New(svc.Pool(), groupID)
	handlers := []consume.Handler{
		consume.Typed("orders.OrderCreated.v1", func(_ context.Context, _ *ordersv1.OrderCreated) error {
			if attempts.Add(1) <= 2 {
				return assert.AnError // transient: NOT an apperr permanent error
			}
			select {
			case applied <- struct{}{}:
			default:
			}
			return nil
		}),
	}

	err = svc.AddBatchConsumer(
		ctx, groupID, []string{topic},
		c.Handler(handlers...),
		func(fb kafka.HandlerFunc) kafka.BatchHandlerFunc {
			return c.BatchHandler(fb, handlers...)
		},
	)
	require.NoError(t, err)

	require.NoError(t, svc.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Stop(stopCtx)
	})

	produceAll(
		t, broker, "svckit-transient-prod-"+suffix,
		orderRecord(t, topic, "t1", "m-t1"),
	)

	// The record is ultimately applied (resolved by retry).
	select {
	case <-applied:
	case <-time.After(30 * time.Second):
		t.Fatal("record was not applied within 30s — transient retry did not resolve")
	}

	// The DLT must stay EMPTY: a transient error resolved by retry never reaches it.
	dltRecs := pollTopic(t, broker, dltTopic, "svckit-transient-reader-"+suffix, 1, 3*time.Second)
	assert.Empty(t, dltRecs, "transient failures resolved by retry must NOT produce a DLT record")
}
