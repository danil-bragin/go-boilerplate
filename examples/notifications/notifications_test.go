package notifications_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"go-boilerplate/examples/notifications"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// captureNotifier is a thread-safe notifier that records all invocations.
type captureNotifier struct {
	mu          sync.Mutex
	invocations [][3]string // each entry: [orderID, paymentID, status]
}

func (c *captureNotifier) Notify(orderID, paymentID, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invocations = append(c.invocations, [3]string{orderID, paymentID, status})
}

func (c *captureNotifier) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.invocations)
}

func (c *captureNotifier) Get(i int) [3]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.invocations[i]
}

// buildApp creates a notifications.App wired against the given broker and pool,
// with a custom notifier for test assertions. The app is started and stopped
// automatically via t.Cleanup.
func buildApp(
	t *testing.T,
	broker string,
	notifier *captureNotifier,
) *notifications.App {
	t.Helper()

	t.Setenv("PG_DSN", pgtest.NewDSN(t))
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	// Disable telemetry in tests.
	t.Setenv("OTEL_ENABLED", "false")

	ctx := context.Background()
	logBuf := &bytes.Buffer{}

	app, err := notifications.NewApp(
		ctx,
		notifications.WithNotifier(notifier.Notify),
		notifications.WithLogWriter(logBuf),
	)
	require.NoError(t, err)

	app.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = app.Stop(stopCtx)
	})

	return app
}

// producePaymentProcessed publishes a PaymentProcessed event to the
// "payments.events" topic with the canonical message-id header.
func producePaymentProcessed(t *testing.T, broker string, evt *ordersv1.PaymentProcessed) {
	t.Helper()

	payload, err := proto.Marshal(evt)
	require.NoError(t, err)

	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "notifications-test-producer",
	}
	cl, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	ctx := context.Background()
	require.NoError(t, p.Produce(ctx, kafka.Record{
		Topic: "payments.events",
		Key:   []byte(evt.OrderId),
		Value: payload,
		Headers: map[string]string{
			// Use OrderId-PaymentId as the canonical message-id so that
			// duplicate records (same order+payment) hit the inbox dedup path.
			"message-id": evt.OrderId + "-" + evt.PaymentId,
			"event-type": "orders.PaymentProcessed.v1",
		},
	}))
}

// pollUntil polls fn until it returns true or the deadline is exceeded.
func pollUntil(t *testing.T, timeout time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// TestNotifications_ConsumesPaymentProcessed verifies that the notifications
// service processes a PaymentProcessed event and invokes the notifier with the
// correct orderID.
func TestNotifications_ConsumesPaymentProcessed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)

	notifCapture := &captureNotifier{}
	buildApp(t, broker, notifCapture)

	orderID := uuid.New().String()
	paymentID := uuid.New().String()

	producePaymentProcessed(t, broker, &ordersv1.PaymentProcessed{
		OrderId:   orderID,
		PaymentId: paymentID,
		Status:    "processed",
	})

	// Wait until the notifier records exactly one invocation for our order.
	ok := pollUntil(t, 30*time.Second, func() bool {
		return notifCapture.Count() >= 1
	})
	require.True(t, ok, "notifier was not called within timeout")

	got := notifCapture.Get(0)
	assert.Equal(t, orderID, got[0], "order_id mismatch")
	assert.Equal(t, paymentID, got[1], "payment_id mismatch")
	assert.Equal(t, "processed", got[2], "status mismatch")
}

// TestNotifications_DuplicateProcessedOnce verifies that the inbox dedup
// mechanism prevents the notifier from being called more than once when the
// same PaymentProcessed event is produced twice (same OrderId + PaymentId).
func TestNotifications_DuplicateProcessedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)

	notifCapture := &captureNotifier{}
	buildApp(t, broker, notifCapture)

	orderID := uuid.New().String()
	paymentID := uuid.New().String()
	evt := &ordersv1.PaymentProcessed{
		OrderId:   orderID,
		PaymentId: paymentID,
		Status:    "processed",
	}

	// Produce the same event twice — same message-id header means same inbox key.
	producePaymentProcessed(t, broker, evt)
	producePaymentProcessed(t, broker, evt)

	// Wait for at least one invocation so we know the consumer is running.
	ok := pollUntil(t, 30*time.Second, func() bool {
		return notifCapture.Count() >= 1
	})
	require.True(t, ok, "notifier was not called within timeout")

	// Allow extra time for any duplicate to slip through.
	time.Sleep(3 * time.Second)

	// Inbox dedup must ensure the notifier is called exactly once.
	assert.Equal(t, 1, notifCapture.Count(), "inbox dedup must ensure exactly one notification")
}
