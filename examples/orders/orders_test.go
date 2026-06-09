package orders_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/examples/orders/internal/migrations"
	"go-boilerplate/examples/orders/internal/transport"
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
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

// buildService wires all components for tests and returns a consumer run function.
func buildService(t *testing.T, pool *pg.Pool, broker string, commandsTopic string) kafka.HandlerFunc {
	t.Helper()

	// Kafka producer for the relay.
	producerCfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-test-producer",
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
	require.NoError(t, kafka.EnsureTopics(ctx, producerClient, 1, 1, commandsTopic, "orders.events"))

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

	// Build and decorate the handler.
	rawHandler := app.CreateOrderHandler(pool, outboxRepo)
	decoratedHandler := app.DecorateCreateOrderHandler(rawHandler, auditStore)

	// Wire consumer handler.
	cmdHandler := transport.NewCommandHandler(pool, decoratedHandler)

	// Start relay in background.
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	t.Cleanup(func() { cancelRelay() })
	go func() {
		_ = relay.Run(relayCtx)
	}()

	return cmdHandler
}

// runConsumer starts a Kafka consumer in a goroutine, calling the handler for each record.
func runConsumer(t *testing.T, broker, groupID, topic string, handler kafka.HandlerFunc) {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-test-consumer-" + groupID,
		GroupID:  groupID,
	}
	consumer, err := kafka.NewConsumer(cfg, topic)
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

// produceCommand produces a CreateOrderCommand to the given topic.
func produceCommand(t *testing.T, broker, topic string, cmd *ordersv1.CreateOrderCommand) {
	t.Helper()
	payload, err := proto.Marshal(cmd)
	require.NoError(t, err)

	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-test-cmd-producer",
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
		Key:   []byte(cmd.OrderId),
		Value: payload,
		Headers: map[string]string{
			"message-id": cmd.OrderId,
		},
	}))
}

// pollForOrder polls until the order row exists in DB, up to timeout.
func pollForOrder(t *testing.T, pool *pg.Pool, orderID string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := pool.Reader().QueryRow(ctx,
			`select count(*) from orders where id = $1`, orderID).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("order %s not found in DB within %v", orderID, timeout)
}

// consumeEvent consumes from orders.events until an OrderCreated event with the given orderID arrives.
func consumeEvent(t *testing.T, broker, orderID string, timeout time.Duration) *ordersv1.OrderCreated {
	t.Helper()
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-test-event-consumer",
		GroupID:  "orders-test-event-consumer-" + orderID,
	}
	consumer, err := kafka.NewConsumer(cfg, "orders.events")
	require.NoError(t, err)
	defer func() { _ = consumer.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	found := make(chan *ordersv1.OrderCreated, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			var evt ordersv1.OrderCreated
			if err := proto.Unmarshal(r.Value, &evt); err != nil {
				return nil //nolint:nilerr // intentionally skip messages that don't unmarshal as OrderCreated
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
		t.Fatalf("OrderCreated event for order %s not received within %v", orderID, timeout)
		return nil
	}
}

// TestOrders_ConsumesCommandWritesOrderAndEmitsEvent is the primary integration test.
// It produces a CreateOrderCommand, runs the orders service handler, and asserts:
//  1. The order row is created in Postgres with status "created".
//  2. An OrderCreated event is published to "orders.events".
//  3. An audit_log row exists for "order:create".
func TestOrders_ConsumesCommandWritesOrderAndEmitsEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const commandsTopic = "orders.commands"
	handler := buildService(t, pool, broker, commandsTopic)

	// Start a consumer for the commands topic.
	runConsumer(t, broker, "orders-svc-test-1", commandsTopic, handler)

	// Produce a CreateOrderCommand.
	orderID := uuid.New().String()
	cmd := &ordersv1.CreateOrderCommand{
		OrderId:     orderID,
		CustomerId:  "c1",
		AmountCents: 1000,
		Currency:    "USD",
	}

	// Set env for test (needed if the handler uses slog.Default; we just start fresh)
	_ = os.Setenv("LOG_LEVEL", "debug")

	produceCommand(t, broker, commandsTopic, cmd)

	// Assert 1: order row exists with status "created".
	pollForOrder(t, pool, orderID, 15*time.Second)

	ctx := context.Background()
	var status string
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select status from orders where id = $1`, orderID).Scan(&status))
	assert.Equal(t, "created", status)

	// Assert 2: OrderCreated event arrives on orders.events.
	evt := consumeEvent(t, broker, orderID, 15*time.Second)
	require.NotNil(t, evt)
	assert.Equal(t, orderID, evt.OrderId)
	assert.Equal(t, "c1", evt.CustomerId)
	assert.EqualValues(t, 1000, evt.AmountCents)
	assert.Equal(t, "USD", evt.Currency)

	// Assert 3: audit_log row for "order:create".
	var auditCount int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where action = 'order:create' and subject = $1`, orderID).Scan(&auditCount))
	assert.Equal(t, 1, auditCount, "expected exactly one audit_log row for order:create")
}

// TestOrders_DuplicateCommandProcessedOnce verifies inbox deduplication:
// producing the same command (same OrderId) twice results in exactly one
// order row (and at most one OrderCreated event).
func TestOrders_DuplicateCommandProcessedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	pool := newTestPool(t)

	const commandsTopic = "orders.commands"
	handler := buildService(t, pool, broker, commandsTopic)

	runConsumer(t, broker, "orders-svc-test-dedup", commandsTopic, handler)

	orderID := uuid.New().String()
	cmd := &ordersv1.CreateOrderCommand{
		OrderId:     orderID,
		CustomerId:  "c1",
		AmountCents: 2000,
		Currency:    "EUR",
	}

	// Produce the same command twice.
	produceCommand(t, broker, commandsTopic, cmd)
	produceCommand(t, broker, commandsTopic, cmd)

	// Wait for first processing to complete.
	pollForOrder(t, pool, orderID, 15*time.Second)

	// Give a moment for any duplicate to be processed.
	time.Sleep(2 * time.Second)

	// Assert exactly one order row.
	ctx := context.Background()
	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from orders where id = $1`, orderID).Scan(&count))
	assert.Equal(t, 1, count, "inbox dedup must ensure exactly one order row")
}
