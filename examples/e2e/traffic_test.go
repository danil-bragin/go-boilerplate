package e2e

// Traffic-emulation test: the full four-service stack under a seeded,
// adversarial load profile (ramp → plateau → spike) driven by the gateway
// scenario pack (examples/gateway/traffic) through the
// platform/testkit/traffic generator.
//
// CI asserts INVARIANTS, not latency:
//   - every accepted order reaches its expected terminal status;
//   - every idempotency group has exactly one distinct order id (the hard
//     invariant: one key must NEVER yield two orders);
//   - each mismatch-race loser carried the documented 409 code (or was
//     absorbed as 202-with-winner-id within the documented window);
//   - every invalid payload was rejected with VALIDATION_FAILED;
//   - the orders system of record holds exactly one row per accepted id.
//
// Latency assertions (in-process tolerances) run ONLY with
// TRAFFIC_ASSERT_LATENCY=1 — CI runners cannot hold a p99 (see
// docs/testing.md §Traffic emulation).
//
// Reproduction: the seed is logged on every run. To replay the exact
// generation sequence of a red run:
//
//	TRAFFIC_SEED=<seed> go test -p 1 ./examples/e2e/ -run TestE2E_Traffic -count=1

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"go-boilerplate/examples/notifications"
	"go-boilerplate/examples/orders"
	"go-boilerplate/examples/payments"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	gateway "go-boilerplate/examples/gateway"
	gwtraffic "go-boilerplate/examples/gateway/traffic"
	kit "go-boilerplate/platform/testkit/traffic"
)

func TestE2E_Traffic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping traffic e2e test in short mode")
	}

	ctx := context.Background()

	// --- Infrastructure: shared Redpanda + 4 Postgres databases ---
	broker, _ := kafkatest.NewRedpanda(t)
	gatewayDSN := pgtest.NewDSN(t)
	ordersDSN := pgtest.NewDSN(t)
	paymentsDSN := pgtest.NewDSN(t)
	notificationsDSN := pgtest.NewDSN(t)

	// --- Start the four services in-process (same wiring as e2e_test.go) ---
	t.Setenv("PG_DSN", notificationsDSN)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "traffic-notifications-"+uuid.New().String())
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	notifApp, err := notifications.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, notifApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = notifApp.Stop(stopCtx)
	})

	t.Setenv("PG_DSN", paymentsDSN)
	t.Setenv("KAFKA_CLIENT_ID", "traffic-payments-"+uuid.New().String())
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
	t.Setenv("KAFKA_CLIENT_ID", "traffic-orders-"+uuid.New().String())
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
	t.Setenv("KAFKA_CLIENT_ID", "traffic-gateway-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "true")
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	// The per-IP token bucket (default 50 rps / burst 100) would throttle
	// the generator and the verify probes — this test measures the
	// pipeline's correctness under load, not the limiter.
	t.Setenv("RATELIMIT_RPS", "100000")
	t.Setenv("RATELIMIT_BURST", "100000")
	// No Redis in this stack: SSE degrades to store polling. Tighten the
	// poll so SSE streams observe terminal statuses promptly.
	t.Setenv("GATEWAY_SSE_POLL_INTERVAL", "500ms")
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

	// --- Generator: gateway scenario pack, ramp → plateau → spike ---
	// No client-level Timeout: SSE streams are long-lived; every other call
	// carries a per-request context deadline inside the pack. A roomy idle
	// pool avoids connection churn at the 80 rps spike.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	client := &http.Client{Transport: transport}

	seed := int64(0) // 0 → time-derived; see the reproduction note above
	if v := os.Getenv("TRAFFIC_SEED"); v != "" {
		seed, err = strconv.ParseInt(v, 10, 64)
		require.NoError(t, err, "TRAFFIC_SEED must be an int64")
	}

	gen, err := kit.NewGenerator(kit.Config{
		Seed:    seed,
		Workers: 32,
		Phases: []kit.Phase{
			{Rate: 10, Duration: 5 * time.Second},  // ramp
			{Rate: 40, Duration: 20 * time.Second}, // plateau
			{Rate: 80, Duration: 5 * time.Second},  // spike
		},
	}, gwtraffic.Pack(baseURL, client, ""))
	require.NoError(t, err)

	ledger := kit.NewLedger()
	runStart := time.Now()
	result, err := gen.Run(ctx, ledger)
	require.NoError(t, err)
	t.Logf("traffic seed: %d (replay with TRAFFIC_SEED=%d)", result.Seed, result.Seed)
	t.Logf("generation+drain took %s\n%s", time.Since(runStart).Round(time.Millisecond), result)

	// Scenario-level failures (transport errors, unexpected statuses) are
	// correctness failures in this in-process setup — the stack is local
	// and the rate limiter is out of the way.
	require.Zero(t, result.TotalFailed(), "scenario failures:\n%s", result)

	// --- Verify invariants against the real stack ---
	ordersPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(ordersDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ordersPool.Close(context.Background()) })

	probes := kit.Probes{
		OrderStatus: gwtraffic.OrderStatusProbe(baseURL, client, ""),
		CountOrders: func(ctx context.Context, ids []string) (int, error) {
			var n int
			err := ordersPool.Reader().
				QueryRow(ctx, `select count(*) from orders where id = any($1::uuid[])`, ids).
				Scan(&n)
			return n, err
		},
	}

	verifyStart := time.Now()
	violations := ledger.Verify(ctx, probes)
	t.Logf("verify took %s", time.Since(verifyStart).Round(time.Millisecond))
	for _, v := range violations {
		t.Errorf("invariant violation: %s", v)
	}
	require.Empty(t, violations, "replay this exact run with TRAFFIC_SEED=%d", result.Seed)

	// --- Latency assertions: opt-in only (TRAFFIC_ASSERT_LATENCY=1) ---
	// CI runners cannot hold an in-process p99; these tolerances are for
	// developer machines (see docs/testing.md §Traffic emulation).
	if os.Getenv("TRAFFIC_ASSERT_LATENCY") == "1" {
		p99, ok := result.Quantile("happy", 0.99)
		require.True(t, ok, "no happy-path latency samples")
		require.Less(t, p99, 1500*time.Millisecond, "POST p99 (in-process)")
		p99read, ok := result.Quantile("reads", 0.99)
		require.True(t, ok, "no read latency samples")
		require.Less(t, p99read, 500*time.Millisecond, "read p99 (in-process)")
	}
}
