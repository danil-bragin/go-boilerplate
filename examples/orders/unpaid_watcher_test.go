package orders_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go-boilerplate/examples/orders"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// timeoutCollector records every OrderPaymentTimedOut event seen on
// orders.events, keyed by order id.
type timeoutCollector struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *timeoutCollector) add(orderID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[orderID]++
}

func (c *timeoutCollector) count(orderID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[orderID]
}

// startTimeoutCollector consumes orders.events from the beginning and records
// all OrderPaymentTimedOut events until the test ends.
func startTimeoutCollector(t *testing.T, broker string) *timeoutCollector {
	t.Helper()
	col := &timeoutCollector{}
	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-timeout-collector",
		GroupID:  "orders-timeout-collector-" + uuid.New().String(),
	}
	consumer, err := kafka.NewConsumer(cfg, []string{"orders.events"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close(context.Background())
	})
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			if r.Headers["event-type"] != "orders.OrderPaymentTimedOut.v1" {
				return nil
			}
			var evt ordersv1.OrderPaymentTimedOut
			if err := proto.Unmarshal(r.Value, &evt); err != nil {
				return nil //nolint:nilerr // skip undecodable records
			}
			col.add(evt.GetOrderId())
			return nil
		})
	}()
	return col
}

// producePaymentEvent publishes a payment outcome event to payments.events.
func producePaymentEvent(t *testing.T, broker, eventType string, msg proto.Message, orderID string) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	require.NoError(t, err)

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "orders-test-payment-producer",
	})
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	require.NoError(t, p.Produce(context.Background(), kafka.Record{
		Topic: "payments.events",
		Key:   []byte(orderID),
		Value: payload,
		Headers: map[string]string{
			"message-id": eventType + "-" + orderID,
			"event-type": eventType,
		},
	}))
}

// pollOrderRowStatus polls until the order row reaches the wanted status.
func pollOrderRowStatus(t *testing.T, pool *pg.Pool, orderID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got string
	for time.Now().Before(deadline) {
		err := pool.Reader().QueryRow(context.Background(),
			`select status from orders where id = $1`, orderID).Scan(&got)
		if err == nil && got == want {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("order %s did not reach status %q within %v (last=%q)", orderID, want, timeout, got)
}

// TestOrders_UnpaidWatcherEmitsTimeoutOnce drives the full orders app with a
// short payment deadline and asserts:
//  1. An order that got paid before the deadline never produces a timeout.
//  2. An order that stays unpaid past the deadline produces EXACTLY ONE
//     OrderPaymentTimedOut event (idempotent across watcher re-polls) and is
//     flagged payment_timeout_emitted in the DB.
func TestOrders_UnpaidWatcherEmitsTimeoutOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "orders-watcher-test-"+uuid.New().String())
	t.Setenv("ORDERS_COMMANDS_TOPIC", "orders.commands")
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("LOG_LEVEL", "error")
	// Short watcher knobs: orders unpaid after 5s are timed out; checked every 300ms.
	t.Setenv("ORDERS_PAYMENT_DEADLINE", "5s")
	t.Setenv("ORDERS_UNPAID_CHECK_INTERVAL", "300ms")

	ctx := context.Background()
	app, err := orders.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = app.Stop(stopCtx)
	})

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	collector := startTimeoutCollector(t, broker)

	// --- Order that gets PAID before the deadline: no timeout ever. ---
	paidID := uuid.New().String()
	produceCommand(t, broker, "orders.commands", &ordersv1.CreateOrderCommand{
		OrderId: paidID, CustomerId: "c-paid", AmountCents: 500, Currency: "USD",
	})
	pollForOrder(t, pool, paidID, 30*time.Second)
	producePaymentEvent(t, broker, "orders.PaymentProcessed.v1", &ordersv1.PaymentProcessed{
		OrderId: paidID, PaymentId: uuid.New().String(), Status: "processed",
	}, paidID)
	// Orders service must record the payment outcome on its own row (this is
	// what makes the watcher a purely local query).
	pollOrderRowStatus(t, pool, paidID, "paid", 10*time.Second)

	// --- Order that stays UNPAID: exactly one timeout after the deadline. ---
	unpaidID := uuid.New().String()
	produceCommand(t, broker, "orders.commands", &ordersv1.CreateOrderCommand{
		OrderId: unpaidID, CustomerId: "c-unpaid", AmountCents: 700, Currency: "USD",
	})
	pollForOrder(t, pool, unpaidID, 30*time.Second)

	require.Eventually(t, func() bool {
		return collector.count(unpaidID) >= 1
	}, 30*time.Second, 200*time.Millisecond, "OrderPaymentTimedOut for the unpaid order was not emitted")

	// The emitted flag must be set so re-polls never emit again.
	var emitted bool
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select payment_timeout_emitted from orders where id = $1`, unpaidID).Scan(&emitted))
	assert.True(t, emitted, "payment_timeout_emitted must be set after the event is enqueued")

	// Let several more watcher ticks pass: still exactly one event.
	time.Sleep(2 * time.Second)
	assert.Equal(t, 1, collector.count(unpaidID),
		"timeout event must be emitted exactly once (idempotent re-poll)")
	assert.Zero(t, collector.count(paidID),
		"an order paid before the deadline must never time out")
}
