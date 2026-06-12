package e2e

// Batch-apply projection throughput measurement (opt-in).
//
// This is the before/after benchmark the batch-apply phase requires: it boots
// the SAME four-service stack twice at a FIXED high input rate for a fixed
// duration — once per-event (GATEWAY_PROJECTION_BATCH=false), once batch
// (GATEWAY_PROJECTION_BATCH=true) — and LOGS committed projection throughput
// (orders that reached a terminal status in the gateway read model / sec) plus
// the run's p50/p95/p99 from the traffic Result.
//
// It is a MEASUREMENT, not a gate: a loaded dev machine is noisy, so we never
// assert B>A. It is gated behind BATCH_THROUGHPUT=1 (mirroring how
// TRAFFIC_ASSERT_LATENCY gates the latency asserts in traffic_test.go) so the
// default suite and CI never pay for it:
//
//	BATCH_THROUGHPUT=1 go test -p 1 ./examples/e2e/ -run TestE2E_BatchThroughput -count=1 -v
//
// Each run still verifies correctness invariants (zero scenario failures, zero
// ledger violations) so a throughput number is never reported for a broken run.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
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

// throughputResult is one run's measured numbers, logged in the before/after
// block.
type throughputResult struct {
	mode          string // "per-event" | "batch"
	elapsed       time.Duration
	terminal      int           // orders_read rows that reached a terminal status
	throughput    float64       // terminal / elapsed-seconds
	accepted      int           // happy+decline OK responses (input that should converge)
	p50, p95, p99 time.Duration // happy-path POST latency
}

func TestE2E_BatchThroughput(t *testing.T) {
	if os.Getenv("BATCH_THROUGHPUT") != "1" {
		t.Skip("opt-in measurement; set BATCH_THROUGHPUT=1 to run the batch-apply before/after benchmark")
	}
	if testing.Short() {
		t.Skip("skipping throughput measurement in short mode")
	}

	// A fixed, modest-but-high load profile reused for BOTH runs so the only
	// variable is the projection mode. Sized to finish each run in ~30-40s of
	// generation plus drain — a couple of minutes total on one machine.
	phases := []kit.Phase{
		{Rate: 60, Duration: 25 * time.Second},
		{Rate: 90, Duration: 10 * time.Second},
	}

	perEvent := runThroughput(t, "per-event", false, phases)
	batch := runThroughput(t, "batch", true, phases)

	// --- before/after block (for the commit message + docs) ---
	t.Logf(
		"\n"+
			"=================== BATCH-APPLY PROJECTION: BEFORE / AFTER ===================\n"+
			"  load: %s\n"+
			"  %-10s | terminal=%-5d accepted=%-5d elapsed=%-7s throughput=%6.1f orders/s | POST p50=%s p95=%s p99=%s\n"+
			"  %-10s | terminal=%-5d accepted=%-5d elapsed=%-7s throughput=%6.1f orders/s | POST p50=%s p95=%s p99=%s\n"+
			"  delta: %+.1f%% committed projection throughput (batch vs per-event)\n"+
			"=============================================================================",
		describePhases(phases),
		perEvent.mode, perEvent.terminal, perEvent.accepted, perEvent.elapsed.Round(time.Millisecond), perEvent.throughput,
		perEvent.p50.Round(time.Millisecond), perEvent.p95.Round(time.Millisecond), perEvent.p99.Round(time.Millisecond),
		batch.mode, batch.terminal, batch.accepted, batch.elapsed.Round(time.Millisecond), batch.throughput,
		batch.p50.Round(time.Millisecond), batch.p95.Round(time.Millisecond), batch.p99.Round(time.Millisecond),
		pctDelta(perEvent.throughput, batch.throughput),
	)
}

func describePhases(phases []kit.Phase) string {
	var b strings.Builder
	for i, p := range phases {
		if i > 0 {
			b.WriteString(" → ")
		}
		fmt.Fprintf(&b, "%grps×%s", p.Rate, p.Duration)
	}
	return b.String()
}

func pctDelta(before, after float64) float64 {
	if before == 0 {
		return 0
	}
	return (after - before) / before * 100
}

// runThroughput boots the full four-service stack with the given projection
// mode, drives the fixed load, verifies correctness, and measures committed
// terminal projection throughput against the gateway read model.
func runThroughput(t *testing.T, mode string, batch bool, phases []kit.Phase) throughputResult {
	t.Helper()
	ctx := context.Background()

	broker, _ := kafkatest.NewRedpanda(t)
	gatewayDSN := pgtest.NewDSN(t)
	ordersDSN := pgtest.NewDSN(t)
	paymentsDSN := pgtest.NewDSN(t)
	notificationsDSN := pgtest.NewDSN(t)

	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	// notifications
	t.Setenv("PG_DSN", notificationsDSN)
	t.Setenv("KAFKA_CLIENT_ID", "tput-notifications-"+uuid.New().String())
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	notifApp, err := notifications.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, notifApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = notifApp.Stop(stopCtx)
	})

	// payments
	t.Setenv("PG_DSN", paymentsDSN)
	t.Setenv("KAFKA_CLIENT_ID", "tput-payments-"+uuid.New().String())
	t.Setenv("ORDERS_EVENTS_TOPIC", "orders.events")
	paymentsApp, err := payments.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, paymentsApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = paymentsApp.Stop(stopCtx)
	})

	// orders
	t.Setenv("PG_DSN", ordersDSN)
	t.Setenv("KAFKA_CLIENT_ID", "tput-orders-"+uuid.New().String())
	t.Setenv("ORDERS_COMMANDS_TOPIC", "orders.commands")
	ordersApp, err := orders.NewApp(ctx)
	require.NoError(t, err)
	require.NoError(t, ordersApp.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = ordersApp.Stop(stopCtx)
	})

	// gateway — the only knob that differs between runs is GATEWAY_PROJECTION_BATCH.
	t.Setenv("PG_DSN", gatewayDSN)
	t.Setenv("KAFKA_CLIENT_ID", "tput-gateway-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "true")
	t.Setenv("PAYMENTS_EVENTS_TOPIC", "payments.events")
	t.Setenv("RATELIMIT_RPS", "100000")
	t.Setenv("RATELIMIT_BURST", "100000")
	t.Setenv("GATEWAY_SSE_POLL_INTERVAL", "500ms")
	if batch {
		t.Setenv("GATEWAY_PROJECTION_BATCH", "true")
	} else {
		t.Setenv("GATEWAY_PROJECTION_BATCH", "false")
	}
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

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 128
	client := &http.Client{Transport: transport}

	gen, err := kit.NewGenerator(kit.Config{
		Seed:    0,
		Workers: 64,
		Phases:  phases,
	}, gwtraffic.Pack(baseURL, client, ""))
	require.NoError(t, err)

	gatewayPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(gatewayDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gatewayPool.Close(context.Background()) })

	const terminalSQL = `select count(*) from orders_read
		where status in ('paid','payment_failed','payment_timed_out')`
	countTerminal := func() int {
		var n int
		require.NoError(t, gatewayPool.Reader().QueryRow(ctx, terminalSQL).Scan(&n))
		return n
	}

	// runStart anchors the throughput measurement at the first generated request
	// so throughput is committed terminal projection rows over the WHOLE pipeline
	// wall-clock (generation + choreography + projection convergence), not just a
	// post-drain residual tail. This is what actually moves when commits-per-poll
	// drops in batch mode.
	ledger := kit.NewLedger()
	runStart := time.Now()
	result, err := gen.Run(ctx, ledger)
	require.NoError(t, err)
	t.Logf("[%s] traffic seed=%d\n%s", mode, result.Seed, result)
	require.Zero(t, result.TotalFailed(), "[%s] scenario failures:\n%s", mode, result)

	// --- Measure committed projection throughput against the gateway read model. ---
	// Poll orders_read terminal rows until the count stops growing (projection
	// has caught up with the input), then divide terminal rows by the wall-clock
	// from runStart to convergence. Terminal statuses are paid / payment_failed /
	// payment_timed_out (everything the choreography settles to).
	prev := -1
	stable := 0
	convergedAt := time.Now()
	for {
		n := countTerminal()
		if n == prev {
			stable++
			if stable == 1 {
				convergedAt = time.Now() // first read at the final count
			}
			if stable >= 3 { // three consecutive equal reads → caught up
				break
			}
		} else {
			stable = 0
			prev = n
		}
		if time.Since(runStart) > 180*time.Second {
			t.Logf("[%s] WARN: projection did not converge within 180s (terminal=%d)", mode, n)
			convergedAt = time.Now()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	elapsed := convergedAt.Sub(runStart)
	terminal := countTerminal()

	// Verify correctness so a throughput number is never reported for a broken
	// run. CountOrders cross-checks the orders system of record.
	ordersPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(ordersDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ordersPool.Close(context.Background()) })
	violations := ledger.Verify(ctx, kit.Probes{
		OrderStatus: gwtraffic.OrderStatusProbe(baseURL, client, ""),
		CountOrders: func(ctx context.Context, ids []string) (int, error) {
			var n int
			err := ordersPool.Reader().
				QueryRow(ctx, `select count(*) from orders where id = any($1::uuid[])`, ids).
				Scan(&n)
			return n, err
		},
	})
	for _, v := range violations {
		t.Errorf("[%s] invariant violation: %s", mode, v)
	}
	require.Empty(t, violations, "[%s] replay with TRAFFIC_SEED=%d", mode, result.Seed)

	happy := result.Scenario("happy")
	decline := result.Scenario("decline")
	accepted := happy.OK + decline.OK
	p50, _ := result.Quantile("happy", 0.50)
	p95, _ := result.Quantile("happy", 0.95)
	p99, _ := result.Quantile("happy", 0.99)

	tput := 0.0
	if elapsed.Seconds() > 0 {
		tput = float64(terminal) / elapsed.Seconds()
	}

	return throughputResult{
		mode:       mode,
		elapsed:    elapsed,
		terminal:   terminal,
		throughput: tput,
		accepted:   accepted,
		p50:        p50,
		p95:        p95,
		p99:        p99,
	}
}
