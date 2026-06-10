package e2e

// Chaos test: a full Kafka-broker outage in the middle of an order flow.
//
// OUTAGE MECHANISM — docker pause/unpause of the Redpanda container, not a
// toxiproxy in front of it: Kafka clients bootstrap via the seed broker but
// then redirect every fetch/produce to the broker's ADVERTISED listener,
// which testcontainers points straight at the container's mapped port — a
// proxy on the seed address is silently bypassed, so "disabling" it cuts
// nothing. Freezing the container (SIGSTOP semantics) cuts every connection
// mid-flight for all clients at once, which is exactly the partition being
// simulated, and unpausing restores the SAME broker (same ports, same data).
//
// Asserted invariants after the broker heals:
//   - ZERO LOSS: the order accepted just before the outage still completes
//     (created → paid end to end) and the projection row is correct.
//   - ZERO DUPLICATE SIDE EFFECTS: exactly one orders row, exactly one
//     payments row, exactly one notification per order — at-least-once
//     redelivery storms after the outage are absorbed by the inbox dedup.
//
// Deadlines are generous on purpose: broker recovery + outbox-relay backoff
// (capped exponential, base 100ms) + consumer reconnect can add up to tens
// of seconds on a loaded CI runner. See docs/testing.md §Chaos testing.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"go-boilerplate/examples/notifications"
	"go-boilerplate/examples/orders"
	"go-boilerplate/examples/payments"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	gateway "go-boilerplate/examples/gateway"
)

// countFor returns how many notification invocations were recorded for the
// given order (defined on the captureNotifier from e2e_test.go — same package).
func (c *captureNotifier) countFor(orderID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, inv := range c.invocations {
		if inv[0] == orderID {
			n++
		}
	}
	return n
}

// brokerOutage pauses/unpauses the Redpanda container via the Docker API.
type brokerOutage struct {
	cli         *testcontainers.DockerClient
	containerID string
}

func newBrokerOutage(t *testing.T, containerID string) *brokerOutage {
	t.Helper()
	cli, err := testcontainers.NewDockerClientWithOpts(context.Background())
	require.NoError(t, err, "chaos: docker client")
	return &brokerOutage{cli: cli, containerID: containerID}
}

func (b *brokerOutage) cut(t *testing.T) {
	t.Helper()
	_, err := b.cli.ContainerPause(context.Background(), b.containerID, mobyclient.ContainerPauseOptions{})
	require.NoError(t, err, "chaos: pause redpanda")
	t.Log("chaos: broker PAUSED")
}

func (b *brokerOutage) heal(t *testing.T) {
	t.Helper()
	_, err := b.cli.ContainerUnpause(context.Background(), b.containerID, mobyclient.ContainerUnpauseOptions{})
	require.NoError(t, err, "chaos: unpause redpanda")
	t.Log("chaos: broker UNPAUSED")
}

// postOrderChaos POSTs an order and returns its id.
func postOrderChaos(t *testing.T, baseURL string, amountCents int64) string {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"customer_id": "chaos-cust", "amount_cents": amountCents, "currency": "USD",
	})
	require.NoError(t, err)
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "POST /v1/orders must be accepted")
	var out struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.OrderID)
	return out.OrderID
}

// rowCount runs a one-row count query against dsn.
func rowCount(t *testing.T, pool *pg.Pool, query, arg string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, pool.Reader().QueryRow(context.Background(), query, arg).Scan(&n))
	return n
}

// TestE2E_ChaosBrokerOutageMidFlow drives the full four-service stack, kills
// the broker mid-flow, heals it, and asserts zero loss / zero duplicates.
func TestE2E_ChaosBrokerOutageMidFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos integration test in short mode")
	}

	ctx := context.Background()

	// --- Infrastructure: Redpanda (handle kept for pause/unpause) + 4 DBs ---
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	require.NoError(t, err, "chaos: start redpanda")
	t.Cleanup(func() { _ = rp.Terminate(context.Background()) })
	broker, err := rp.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	outage := newBrokerOutage(t, rp.GetContainerID())

	gatewayDSN := pgtest.NewDSN(t)
	ordersDSN := pgtest.NewDSN(t)
	paymentsDSN := pgtest.NewDSN(t)
	notificationsDSN := pgtest.NewDSN(t)

	notifCapture := &captureNotifier{}

	// --- Start the four services in-process (same wiring as e2e_test.go) ---
	t.Setenv("PG_DSN", notificationsDSN)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "chaos-notifications-"+uuid.New().String())
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("DRAIN_GRACE", "0")

	notifApp, err := notifications.NewApp(ctx, notifications.WithNotifier(notifCapture.add))
	require.NoError(t, err)
	require.NoError(t, notifApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = notifApp.Stop(stopCtx)
	})

	t.Setenv("PG_DSN", paymentsDSN)
	t.Setenv("KAFKA_CLIENT_ID", "chaos-payments-"+uuid.New().String())
	t.Setenv("ORDERS_EVENTS_TOPIC", "orders.events")
	paymentsApp, err := payments.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, paymentsApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = paymentsApp.Stop(stopCtx)
	})

	t.Setenv("PG_DSN", ordersDSN)
	t.Setenv("KAFKA_CLIENT_ID", "chaos-orders-"+uuid.New().String())
	t.Setenv("ORDERS_COMMANDS_TOPIC", "orders.commands")
	ordersApp, err := orders.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, ordersApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = ordersApp.Stop(stopCtx)
	})

	t.Setenv("PG_DSN", gatewayDSN)
	t.Setenv("KAFKA_CLIENT_ID", "chaos-gateway-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "true")
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	gatewayApp, err := gateway.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, gatewayApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = gatewayApp.Stop(stopCtx)
	})

	baseURL := "http://" + gatewayApp.Addr()
	adminURL := "http://" + gatewayApp.AdminAddr()
	require.Eventually(t, func() bool {
		resp, err := http.Get(adminURL + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 300*time.Millisecond, "gateway did not become ready")

	// --- Flow A: a full happy path BEFORE the outage (stack sanity). ---
	t.Log("chaos: flow A (pre-outage)")
	orderA := postOrderChaos(t, baseURL, 1500)
	require.True(t, pollUntil(60*time.Second, func() bool {
		return getOrderStatus(t, baseURL, orderA) == "paid"
	}), "flow A must complete before the outage (got %q)", getOrderStatus(t, baseURL, orderA))

	// --- Flow B: accepted, then the broker dies mid-flow. ---
	t.Log("chaos: flow B (POST, then cut the broker)")
	orderB := postOrderChaos(t, baseURL, 2500)
	outage.cut(t)

	// Outage window: long enough that the outbox relay enters capped
	// exponential backoff and consumers lose their fetch sessions.
	const outageWindow = 20 * time.Second
	time.Sleep(outageWindow)
	outage.heal(t)

	// --- Zero loss: flow B completes after the heal. ---
	// Budget covers consumer reconnect + relay backoff (≤30s cap) + the
	// normal choreography latency.
	t.Log("chaos: waiting for flow B to complete after heal")
	require.True(t, pollUntil(120*time.Second, func() bool {
		return getOrderStatus(t, baseURL, orderB) == "paid"
	}), "flow B must complete after the broker heals (got %q)", getOrderStatus(t, baseURL, orderB))

	// Let post-heal redeliveries (relay re-publishes, rebalance replays)
	// settle before counting side effects.
	time.Sleep(5 * time.Second)

	// --- Zero duplicate side effects. ---
	ordersPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(ordersDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ordersPool.Close(context.Background()) })
	paymentsPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(paymentsDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = paymentsPool.Close(context.Background()) })
	gatewayPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(gatewayDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gatewayPool.Close(context.Background()) })

	for _, orderID := range []string{orderA, orderB} {
		require.EqualValues(t, 1,
			rowCount(t, ordersPool, `select count(*) from orders where id = $1`, orderID),
			"exactly one orders row for %s", orderID)
		require.EqualValues(t, 1,
			rowCount(t, paymentsPool, `select count(*) from payments where order_id = $1`, orderID),
			"exactly one payment processed for %s (inbox dedup must absorb redeliveries)", orderID)
		require.EqualValues(t, 1,
			rowCount(t, gatewayPool, `select count(*) from orders_read where order_id = $1`, orderID),
			"exactly one projection row for %s", orderID)
		require.Equal(t, "paid", getOrderStatus(t, baseURL, orderID),
			"projection status for %s", orderID)
		require.Equal(t, 1, notifCapture.countFor(orderID),
			"exactly one notification for %s", orderID)
	}
}
