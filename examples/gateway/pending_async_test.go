package gateway_test

// Write-path relief tests (round 3, T2):
//   - idempotency lookups route through the READER pool (pg.FromContextRead);
//     with the default reader==writer config the 202-retry/409-mismatch
//     semantics are unchanged, and an explicitly configured PG_READER_DSN
//     exercises the distinct-reader code path;
//   - GATEWAY_PENDING_ASYNC=true batches pending-row inserts: a POST burst
//     eventually lands every row, and shutdown drains the buffer.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gateway "go-boilerplate/examples/gateway"
	"go-boilerplate/examples/gateway/internal/api"
	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// TestGateway_IdempotencyReaderPoolRouting proves the idempotency lookup
// works against a DISTINCT reader pool (PG_READER_DSN set; same database, so
// zero lag): retry with the same key → same id + 202; reuse with a different
// body → 409. Same semantics as the default reader==writer config — the
// reader routing must not change observable behavior when lag is zero.
func TestGateway_IdempotencyReaderPoolRouting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	// Distinct reader pool pointed at the same database (lag-free replica).
	t.Setenv("PG_READER_DSN", dsn)
	baseURL := startApp(t, broker, dsn)

	st, id := postOrderRaw(t, baseURL, "reader-key", "",
		[]byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)
	require.NotEmpty(t, id)

	// True retry → same deterministic id, 202.
	st, retryID := postOrderRaw(t, baseURL, "reader-key", "",
		[]byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)
	assert.Equal(t, id, retryID, "retry through the reader pool must return the same order id")

	// Body mismatch → 409 (the reader sees the pending row — same DB).
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders",
		bytes.NewReader([]byte(`{"customer_id":"c1","amount_cents":9999,"currency":"USD"}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "reader-key")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode,
		"body mismatch must still 409 when the idempotency read runs on the reader pool")
}

// TestGateway_PendingAsyncBurstEventuallyPersisted enables
// GATEWAY_PENDING_ASYNC and bursts 200 POSTs: every accepted order's pending
// row must EVENTUALLY appear in orders_read (batched flush ≤50ms/≤100 rows),
// proving the async path loses nothing under burst within buffer capacity.
func TestGateway_PendingAsyncBurstEventuallyPersisted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("GATEWAY_PENDING_ASYNC", "true")
	// The burst comes from one client IP — lift the per-IP rate limit so the
	// test exercises the batcher, not the limiter.
	t.Setenv("RATELIMIT_RPS", "10000")
	t.Setenv("RATELIMIT_BURST", "10000")
	baseURL := startApp(t, broker, dsn)

	const total = 200
	const workers = 20

	ids := make([]string, total)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < total; i += workers {
				body := []byte(`{"customer_id":"burst","amount_cents":100,"currency":"USD"}`)
				req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders", bytes.NewReader(body))
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				var out struct {
					OrderID string `json:"order_id"`
				}
				if resp.StatusCode == http.StatusAccepted {
					_ = json.NewDecoder(resp.Body).Decode(&out)
					ids[i] = out.OrderID
				}
				resp.Body.Close()
			}
		}(w)
	}
	wg.Wait()

	accepted := make([]uuid.UUID, 0, total)
	for _, id := range ids {
		require.NotEmpty(t, id, "every burst POST must be accepted")
		accepted = append(accepted, uuid.MustParse(id))
	}

	ctx := context.Background()
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(os.Getenv("PG_DSN"))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	// Poll: all pending rows present (eventual, not read-your-writes).
	require.Eventually(t, func() bool {
		var count int
		if err := pool.Reader().QueryRow(ctx,
			`select count(*) from orders_read where order_id = any($1)`, accepted).Scan(&count); err != nil {
			return false
		}
		return count == total
	}, 15*time.Second, 100*time.Millisecond,
		"all %d burst pending rows must eventually be flushed by the async batcher", total)
}

// TestPendingBatcher_DrainFlushesBufferOnStop proves the graceful-drain
// contract directly: with a flush interval too long to ever tick, rows
// buffered before cancellation are still flushed before Run returns.
func TestPendingBatcher_DrainFlushesBufferOnStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, gateway.Migrations, "sql"))

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	// 1h flush interval: only the shutdown drain can explain flushed rows.
	b := api.NewPendingBatcher(pool, slog.New(slog.DiscardHandler),
		api.WithPendingFlushInterval(time.Hour))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(runCtx)
	}()

	const rows = 10
	want := make([]uuid.UUID, 0, rows)
	for range rows {
		id := uuid.New()
		want = append(want, id)
		b.Enqueue(ctx, storegen.InsertPendingOrderParams{
			OrderID: id, CustomerID: "drain", AmountCents: 1, Currency: "USD",
		})
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("PendingBatcher.Run did not return after cancellation")
	}

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from orders_read where order_id = any($1)`, want).Scan(&count))
	assert.Equal(t, rows, count, "shutdown drain must flush every buffered row")
}
