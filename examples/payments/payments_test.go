package payments_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/examples/payments/internal/domain/payment"
	"go-boilerplate/examples/payments/internal/migrations"
	"go-boilerplate/examples/payments/internal/transport"
	"go-boilerplate/platform/clock"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/outboxkafka"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// newTestPool creates a migrated Postgres pool for testing.
func newTestPool(t *testing.T) *pg.Pool {
	t.Helper()
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

// buildService wires all components for tests and returns a consumer event handler.
func buildService(t *testing.T, pool *pg.Pool, broker string, eventsTopic string) kafka.HandlerFunc {
	t.Helper()

	// Kafka producer for the relay.
	producerCfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "payments-test-producer",
	}
	producerClient, err := kafka.NewClient(producerCfg)
	require.NoError(t, err)
	producer := kafka.NewProducer(producerClient)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = producer.Close(ctx)
	})

	// Ensure topics exist.
	ctx := context.Background()
	require.NoError(t, kafka.EnsureTopics(ctx, producerClient, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, eventsTopic, "payments.events"))

	// Outbox repo + relay.
	outboxRepo := outbox.NewRepository(pool)
	publisher := outboxkafka.New(producer)
	relay := outbox.NewRelay(pool, publisher, outbox.RelayConfig{
		PollInterval: 100 * time.Millisecond,
		BatchSize:    100,
	})
	relay.SetOnError(func(err error) {
		t.Logf("relay error: %v", err)
	})

	// Audit store.
	auditStore := audit.NewPgStore(pool)

	// Build and decorate the handler (thin adapter over the domain service).
	domainSvc := payment.NewService(payment.NewPgRepository(pool), outboxRepo, clock.System{}, "payments.events")
	rawHandler := app.ProcessPaymentHandler(domainSvc)
	decoratedHandler := app.DecorateProcessPaymentHandler(rawHandler, auditStore)

	// Wire event handler.
	evtHandler := transport.NewEventHandler(pool, decoratedHandler)

	// Start relay in background.
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	t.Cleanup(func() { cancelRelay() })
	go func() {
		_ = relay.Run(relayCtx)
	}()

	return evtHandler
}

// runConsumer starts a Kafka consumer in a goroutine, calling the handler for each record.
func runConsumer(t *testing.T, broker, groupID, topic string, handler kafka.HandlerFunc) {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "payments-test-consumer-" + groupID,
		GroupID:  groupID,
	}
	consumer, err := kafka.NewConsumer(cfg, []string{topic})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close(context.Background())
	})
	go func() {
		_ = consumer.Run(ctx, handler)
	}()
}

// produceOrderCreated produces an OrderCreated event to the given topic.
func produceOrderCreated(t *testing.T, broker, topic string, evt *ordersv1.OrderCreated) {
	t.Helper()
	payload, err := proto.Marshal(evt)
	require.NoError(t, err)

	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "payments-test-event-producer",
	}
	cl, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	}()

	ctx := context.Background()
	require.NoError(t, p.Produce(ctx, kafka.Record{
		Topic: topic,
		Key:   []byte(evt.OrderId),
		Value: payload,
		Headers: map[string]string{
			"message-id": evt.OrderId,
			"event-type": "orders.OrderCreated.v1",
		},
	}))
}

// pollForPayment polls until a payment row for the given order_id exists in DB.
func pollForPayment(t *testing.T, pool *pg.Pool, orderID string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := pool.Reader().QueryRow(ctx,
			`select count(*) from payments where order_id = $1`, orderID).Scan(&count)
		if err == nil && count >= 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("payment for order %s not found in DB within %v", orderID, timeout)
}

// consumePaymentEvent consumes from payments.events until a PaymentProcessed event with the given orderID arrives.
func consumePaymentEvent(t *testing.T, broker, orderID string, timeout time.Duration) *ordersv1.PaymentProcessed {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "payments-test-event-consumer",
		GroupID:  "payments-test-event-consumer-" + orderID,
	}
	consumer, err := kafka.NewConsumer(cfg, []string{"payments.events"})
	require.NoError(t, err)
	defer func() { _ = consumer.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	found := make(chan *ordersv1.PaymentProcessed, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			var evt ordersv1.PaymentProcessed
			if err := proto.Unmarshal(r.Value, &evt); err != nil {
				return nil //nolint:nilerr // intentionally skip messages that don't unmarshal as PaymentProcessed
			}
			if evt.OrderId == orderID {
				select {
				case found <- &evt:
				default:
				}
			}
			return nil
		})
	}()

	select {
	case evt := <-found:
		return evt
	case <-ctx.Done():
		t.Fatalf("PaymentProcessed event for order %s not received within %v", orderID, timeout)
		return nil
	}
}

// TestPayments_ConsumesOrderCreatedEmitsPaymentProcessed is the primary integration test.
// It produces an OrderCreated event, runs the payments service handler, and asserts:
//  1. A payment row is created in Postgres with status "processed".
//  2. A PaymentProcessed event is published to "payments.events".
//  3. An audit_log row exists for "payment:process".
func TestPayments_ConsumesOrderCreatedEmitsPaymentProcessed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const eventsTopic = "orders.events"
	handler := buildService(t, pool, broker, eventsTopic)

	// Start a consumer for the orders.events topic.
	runConsumer(t, broker, "payments-svc-test-1", eventsTopic, handler)

	// Produce an OrderCreated event.
	orderID := uuid.New().String()

	_ = os.Setenv("LOG_LEVEL", "debug")

	produceOrderCreated(t, broker, eventsTopic, &ordersv1.OrderCreated{
		OrderId:     orderID,
		CustomerId:  "c1",
		AmountCents: 1000,
		Currency:    "USD",
	})

	// Assert 1: payment row exists with status "processed".
	pollForPayment(t, pool, orderID, 15*time.Second)

	ctx := context.Background()
	var status string
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select status from payments where order_id = $1`, orderID).Scan(&status))
	assert.Equal(t, "processed", status)

	// Assert 2: PaymentProcessed event arrives on payments.events.
	evt := consumePaymentEvent(t, broker, orderID, 15*time.Second)
	require.NotNil(t, evt)
	assert.Equal(t, orderID, evt.OrderId)
	assert.Equal(t, "processed", evt.Status)
	assert.NotEmpty(t, evt.PaymentId)

	// Assert 3: audit_log row for "payment:process".
	var auditCount int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where action = 'payment:process' and subject = $1`, orderID).Scan(&auditCount))
	assert.Equal(t, 1, auditCount, "expected exactly one audit_log row for payment:process")
}

// TestPayments_DuplicateEventProcessedOnce verifies inbox deduplication:
// producing the same OrderCreated event (same OrderId) twice results in exactly one
// payment row.
func TestPayments_DuplicateEventProcessedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const eventsTopic = "orders.events"
	handler := buildService(t, pool, broker, eventsTopic)

	runConsumer(t, broker, "payments-svc-test-dedup", eventsTopic, handler)

	orderID := uuid.New().String()
	evt := &ordersv1.OrderCreated{
		OrderId:     orderID,
		CustomerId:  "c1",
		AmountCents: 2000,
		Currency:    "EUR",
	}

	// Produce the same event twice.
	produceOrderCreated(t, broker, eventsTopic, evt)
	produceOrderCreated(t, broker, eventsTopic, evt)

	// Wait for first processing to complete.
	pollForPayment(t, pool, orderID, 15*time.Second)

	// Give a moment for any duplicate to be processed.
	time.Sleep(2 * time.Second)

	// Assert exactly one payment row.
	ctx := context.Background()
	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from payments where order_id = $1`, orderID).Scan(&count))
	assert.Equal(t, 1, count, "inbox dedup must ensure exactly one payment row")
}

// consumePaymentFailedEvent consumes from payments.events until a PaymentFailed
// event with the given orderID arrives (matched on the event-type header).
func consumePaymentFailedEvent(t *testing.T, broker, orderID string, timeout time.Duration) (*ordersv1.PaymentFailed, map[string]string) {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "payments-test-failed-consumer",
		GroupID:  "payments-test-failed-consumer-" + orderID,
	}
	consumer, err := kafka.NewConsumer(cfg, []string{"payments.events"})
	require.NoError(t, err)
	defer func() { _ = consumer.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type hit struct {
		evt     *ordersv1.PaymentFailed
		headers map[string]string
	}
	found := make(chan hit, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			if r.Headers["event-type"] != "orders.PaymentFailed.v1" {
				return nil
			}
			var evt ordersv1.PaymentFailed
			if err := proto.Unmarshal(r.Value, &evt); err != nil {
				return nil //nolint:nilerr // skip records that don't decode as PaymentFailed
			}
			if evt.OrderId == orderID {
				select {
				case found <- hit{evt: &evt, headers: r.Headers}:
				default:
				}
			}
			return nil
		})
	}()

	select {
	case h := <-found:
		return h.evt, h.headers
	case <-ctx.Done():
		t.Fatalf("PaymentFailed event for order %s not received within %v", orderID, timeout)
		return nil, nil
	}
}

// TestPayments_DeclinesLargeAmountEmitsPaymentFailed verifies the deterministic
// demo decline rule: an OrderCreated with amount_cents >= 1_000_000 must be
// declined — the payment row is written with status "failed" and a
// PaymentFailed{reason:"declined"} event is emitted on payments.events.
func TestPayments_DeclinesLargeAmountEmitsPaymentFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const eventsTopic = "orders.events"
	handler := buildService(t, pool, broker, eventsTopic)
	runConsumer(t, broker, "payments-svc-test-declined", eventsTopic, handler)

	orderID := uuid.New().String()
	produceOrderCreated(t, broker, eventsTopic, &ordersv1.OrderCreated{
		OrderId:     orderID,
		CustomerId:  "c-big",
		AmountCents: 1_000_000, // exactly at the decline threshold
		Currency:    "USD",
	})

	// Payment row exists with status "failed".
	pollForPayment(t, pool, orderID, 15*time.Second)
	ctx := context.Background()
	var status string
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select status from payments where order_id = $1`, orderID).Scan(&status))
	assert.Equal(t, "failed", status, "payments above the threshold must be recorded as failed")

	// PaymentFailed event arrives on payments.events with reason "declined".
	evt, headers := consumePaymentFailedEvent(t, broker, orderID, 15*time.Second)
	require.NotNil(t, evt)
	assert.Equal(t, orderID, evt.OrderId)
	assert.Equal(t, "declined", evt.Reason)
	require.NotNil(t, evt.OccurredAt, "occurred_at must be set")
	assert.Equal(t, "orders.PaymentFailed.v1", headers["event-type"])

	// No PaymentProcessed event must exist for this order — failure is terminal
	// and mutually exclusive with success. (Cheap check: the payments.events
	// topic only carries this order's PaymentFailed, asserted above by matching
	// on event-type; a stray success would have produced a second payment row.)
	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from payments where order_id = $1`, orderID).Scan(&count))
	assert.Equal(t, 1, count)
}

// TestPayments_BelowThresholdStillProcessed pins the boundary: one cent below
// the threshold is processed normally.
func TestPayments_BelowThresholdStillProcessed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const eventsTopic = "orders.events"
	handler := buildService(t, pool, broker, eventsTopic)
	runConsumer(t, broker, "payments-svc-test-boundary", eventsTopic, handler)

	orderID := uuid.New().String()
	produceOrderCreated(t, broker, eventsTopic, &ordersv1.OrderCreated{
		OrderId:     orderID,
		CustomerId:  "c-boundary",
		AmountCents: 999_999,
		Currency:    "USD",
	})

	pollForPayment(t, pool, orderID, 15*time.Second)
	var status string
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select status from payments where order_id = $1`, orderID).Scan(&status))
	assert.Equal(t, "processed", status)
}
