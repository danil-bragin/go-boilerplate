// Package e2e contains the end-to-end choreography integration test for the
// example services. It spins up Redpanda and four Postgres databases
// (one per service), starts all four apps in-process, and drives the full
// flow:
//
//	POST /orders → gateway → orders.commands → orders svc → orders.events →
//	payments svc → payments.events → notifications svc
//	GET /orders/{id}: created → paid
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"go-boilerplate/examples/notifications"
	"go-boilerplate/examples/orders"
	"go-boilerplate/examples/payments"
	"go-boilerplate/platform/kafka/kafkatest"
	"go-boilerplate/platform/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gateway "go-boilerplate/examples/gateway"
)

// captureNotifier records all notification invocations in a thread-safe slice.
type captureNotifier struct {
	mu          sync.Mutex
	invocations [][3]string // [orderID, paymentID, status]
}

func (c *captureNotifier) add(orderID, paymentID, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invocations = append(c.invocations, [3]string{orderID, paymentID, status})
}

func (c *captureNotifier) find(orderID string) ([3]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, inv := range c.invocations {
		if inv[0] == orderID {
			return inv, true
		}
	}
	return [3]string{}, false
}

// pollUntil polls fn until it returns true or the timeout elapses.
func pollUntil(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

// discardWriter wraps io.Discard to satisfy the io.Writer interface.
// Each service gets its own instance to avoid any shared state.
func discardWriter() io.Writer { return io.Discard }

// TestE2E_OrderChoreography is the full end-to-end integration test.
//
// It starts four services in-process (gateway, orders, payments, notifications),
// posts an order via the gateway HTTP API, and asserts the complete choreography:
//
//  1. Gateway accepts POST /orders → 202 + order_id.
//  2. Orders service consumes the command, writes the order row, emits OrderCreated.
//  3. Gateway projection picks up OrderCreated → GET /orders/{id} shows status="created" or "paid".
//  4. Payments service consumes OrderCreated, writes a payment row, emits PaymentProcessed.
//  5. Gateway projection picks up PaymentProcessed → GET /orders/{id} shows status="paid".
//  6. Notifications service consumes PaymentProcessed and fires the notifier.
func TestE2E_OrderChoreography(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
	}

	ctx := context.Background()

	// --- Infrastructure ---
	// One shared Redpanda broker for all services.
	broker, _ := kafkatest.NewRedpanda(t)

	// Four independent Postgres databases (one per service) — isolation without
	// shared schema concerns.
	gatewayDSN := pgtest.NewDSN(t)
	ordersDSN := pgtest.NewDSN(t)
	paymentsDSN := pgtest.NewDSN(t)
	notificationsDSN := pgtest.NewDSN(t)

	notifCapture := &captureNotifier{}

	// --- Wire and start notifications service ---
	// Start first so it is ready to consume PaymentProcessed as soon as the
	// event appears on the broker.
	//
	// ADMIN_HTTP_ADDR=127.0.0.1:0 assigns a random OS port per service so that
	// all four admin servers can bind simultaneously in-process. Without this,
	// all four would default to :9090, only the first would succeed, and
	// gatewayApp.AdminAddr() would return the notifications port — causing the
	// readyz poll to check the wrong service.
	os.Setenv("PG_DSN", notificationsDSN)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "e2e-notifications-"+uuid.New().String())
	os.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	os.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	os.Setenv("OTEL_ENABLED", "false")
	os.Setenv("LOG_LEVEL", "error")

	notifApp, err := notifications.NewApp(
		ctx,
		notifications.WithNotifier(notifCapture.add),
		notifications.WithLogWriter(discardWriter()),
	)
	require.NoError(t, err, "notifications.NewApp failed")
	notifApp.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = notifApp.Stop(stopCtx)
	})

	// --- Wire and start payments service ---
	os.Setenv("PG_DSN", paymentsDSN)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "e2e-payments-"+uuid.New().String())
	os.Setenv("ORDERS_EVENTS_TOPIC", "orders.events")
	os.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	os.Setenv("OTEL_ENABLED", "false")
	os.Setenv("LOG_LEVEL", "error")

	paymentsApp, err := payments.NewApp(
		ctx,
		payments.WithLogWriter(discardWriter()),
	)
	require.NoError(t, err, "payments.NewApp failed")
	paymentsApp.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = paymentsApp.Stop(stopCtx)
	})

	// --- Wire and start orders service ---
	os.Setenv("PG_DSN", ordersDSN)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "e2e-orders-"+uuid.New().String())
	os.Setenv("ORDERS_COMMANDS_TOPIC", "orders.commands")
	os.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	os.Setenv("OTEL_ENABLED", "false")
	os.Setenv("LOG_LEVEL", "error")

	ordersApp, err := orders.NewApp(
		ctx,
		orders.WithLogWriter(discardWriter()),
	)
	require.NoError(t, err, "orders.NewApp failed")
	ordersApp.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ordersApp.Stop(stopCtx)
	})

	// --- Wire and start gateway service ---
	os.Setenv("PG_DSN", gatewayDSN)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "e2e-gateway-"+uuid.New().String())
	os.Setenv("HTTP_ADDR", "127.0.0.1:0") // random port
	os.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	os.Setenv("GATEWAY_AUTH_DISABLED", "true")
	os.Setenv("GATEWAY_COMMANDS_TOPIC", "orders.commands")
	os.Setenv("GATEWAY_ORDERS_EVENTS_TOPIC", "orders.events")
	os.Setenv("GATEWAY_PAYMENTS_EVENTS_TOPIC", "payments.events")
	os.Setenv("OTEL_ENABLED", "false")
	os.Setenv("LOG_LEVEL", "error")

	gatewayApp, err := gateway.NewApp(
		ctx,
		gateway.WithLogWriter(discardWriter()),
	)
	require.NoError(t, err, "gateway.NewApp failed")
	gatewayApp.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = gatewayApp.Stop(stopCtx)
	})

	baseURL := "http://" + gatewayApp.Addr()
	adminURL := "http://" + gatewayApp.AdminAddr()

	// Wait until the gateway's readiness probe reports healthy instead of
	// sleeping a fixed duration. This eliminates the fixed 2 s warm-up delay
	// and makes the test resilient to slow CI environments where 2 s is not
	// enough and fast laptops where it's unnecessarily long.
	//
	// The readyz endpoint is served by the gateway's own admin HTTP server
	// (AdminAddr). Each service is given a distinct random admin port via
	// ADMIN_HTTP_ADDR=127.0.0.1:0 so that gatewayApp.AdminAddr() returns the
	// gateway's actual bound port, not a port owned by another service.
	//
	// NOTE: readyz checks postgres + kafka producer connectivity; it does NOT
	// wait for consumer-group join. kgo starts brand-new groups from offset 0
	// (ConsumeResetOffset AtStart), so a message published just before the group
	// joins will still be consumed.
	require.Eventually(t, func() bool {
		resp, err := http.Get(adminURL + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 300*time.Millisecond, "gateway did not become ready within timeout")

	// --- Step 1: POST /orders → 202 + order_id ---
	t.Log("Step 1: POST /orders")
	reqBody := map[string]interface{}{
		"customer_id":  "c1",
		"amount_cents": int64(1500),
		"currency":     "USD",
	}
	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	resp, err := http.Post(baseURL+"/orders", "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "expected 202 from POST /orders")

	var createResp struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResp))
	orderID := createResp.OrderID
	require.NotEmpty(t, orderID, "expected non-empty order_id in response")
	t.Logf("Step 1 OK: order_id=%s", orderID)

	// --- Step 2: Poll GET /orders/{id} until the projection row appears ---
	// Proves: orders svc consumed the command, emitted OrderCreated,
	//         gateway projection applied OrderCreated.
	//
	// We accept either "created" or "paid" because the outbox relay + payments
	// consumer can advance the status to "paid" before the next poll fires
	// (relay interval 200 ms, poll interval 300 ms). Both statuses prove the
	// full chain up through the projection ran correctly.
	t.Log("Step 2: waiting for projection row to appear (orders→OrderCreated→gateway projection)")
	ok := pollUntil(60*time.Second, func() bool {
		s := getOrderStatus(t, baseURL, orderID)
		return s == "created" || s == "paid"
	})
	require.True(t, ok, "order %s did not appear in the projection within timeout", orderID)
	t.Logf("Step 2 OK: order %s is in projection (status=%s)", orderID, getOrderStatus(t, baseURL, orderID))

	// --- Step 3: Poll GET /orders/{id} until status=="paid" ---
	// Proves: payments svc consumed OrderCreated, emitted PaymentProcessed,
	//         gateway projection applied PaymentProcessed.
	t.Log("Step 3: waiting for status=paid (payments→PaymentProcessed→gateway projection)")
	ok = pollUntil(60*time.Second, func() bool {
		return getOrderStatus(t, baseURL, orderID) == "paid"
	})
	require.True(t, ok, "order %s did not reach status 'paid' within timeout", orderID)
	t.Logf("Step 3 OK: order %s is paid", orderID)

	// --- Step 4: Assert notifications notifier was invoked for this order ---
	// Proves: notifications svc consumed PaymentProcessed.
	t.Log("Step 4: waiting for notification (notifications→PaymentProcessed notifier)")
	ok = pollUntil(30*time.Second, func() bool {
		_, found := notifCapture.find(orderID)
		return found
	})
	require.True(t, ok, "notification for order %s was not captured within timeout", orderID)
	inv, _ := notifCapture.find(orderID)
	assert.Equal(t, orderID, inv[0], "notification order_id mismatch")
	assert.NotEmpty(t, inv[1], "notification payment_id should be non-empty")
	assert.Equal(t, "processed", inv[2], "notification status should be 'processed'")
	t.Logf("Step 4 OK: notification captured for order %s (payment_id=%s, status=%s)", orderID, inv[1], inv[2])
}

// getOrderStatus calls GET /orders/{id} and returns the status string, or "" on
// any error or non-200 response (caller retries via pollUntil).
func getOrderStatus(t *testing.T, baseURL, orderID string) string {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, orderID))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var view struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		return ""
	}
	return view.Status
}
