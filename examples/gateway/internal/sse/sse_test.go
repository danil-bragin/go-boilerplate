package sse_test

// Integration tests for the shared-subscription SSE streamer: one Redis
// subscriber per Streamer replica fans broadcast status updates out to an
// in-process registry of streams, so Redis connections do NOT scale with the
// number of open streams (the old per-stream Dedicate() hit rueidis'
// BlockingPoolSize at 1024 streams, then redis maxclients globally).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"

	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/sse"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/platform/testkit/goleakopts"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.TerminateShared()
	os.Exit(code)
}

// newRedisAddr starts a Redis testcontainer and returns its host:port.
func newRedisAddr(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Terminate(context.Background()) })
	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	return strings.TrimPrefix(addr, "redis://")
}

// newTestRedis returns a rueidis client owned by the TEST (separate from the
// streamer's client) for CLIENT LIST / CLIENT KILL / PUBSUB introspection.
func newTestRedis(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// env is one Streamer under test: real Postgres (migrated gateway schema),
// optional real Redis, and an httptest server mounting Stream.
type env struct {
	pool      *pg.Pool
	client    rueidis.Client // nil = poll mode
	streamer  *sse.Streamer
	srv       *httptest.Server
	base      string
	closeOnce sync.Once
}

// newEnv builds the env. redisAddr == "" means no-Redis poll mode. Cleanup is
// registered with t.Cleanup but can also be invoked early via e.close()
// (idempotent) — the goleak test needs everything torn down before its
// deferred VerifyNone runs.
func newEnv(t *testing.T, dsn, redisAddr string, opts ...sse.Option) *env {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)

	var client rueidis.Client
	if redisAddr != "" {
		client, err = rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisAddr}})
		require.NoError(t, err)
	}

	logger := slog.New(slog.DiscardHandler)
	streamer := sse.New(client, pool, logger, true /* authDisabled */, opts...)

	r := chi.NewRouter()
	r.Get("/v1/orders/{id}/events", streamer.Stream)
	srv := httptest.NewServer(r)

	e := &env{pool: pool, client: client, streamer: streamer, srv: srv, base: srv.URL}
	t.Cleanup(e.close)
	return e
}

func (e *env) close() {
	e.closeOnce.Do(func() {
		e.streamer.Shutdown() // ends active streams; stops the shared subscriber
		e.srv.Close()
		if e.client != nil {
			e.client.Close()
		}
		_ = e.pool.Close(context.Background())
	})
}

func (e *env) seedOrder(t *testing.T, orderID, status string) {
	t.Helper()
	_, err := e.pool.Writer().Exec(context.Background(),
		`insert into orders_read (order_id, customer_id, amount_cents, currency, status)
		 values ($1, 'cust-sse', 100, 'USD', $2)`, orderID, status)
	require.NoError(t, err)
}

// setStatus updates the read-model row WITHOUT publishing — callers decide
// whether to Notify (normal path) or not (simulating a missed broadcast).
func (e *env) setStatus(t *testing.T, orderID, status string) {
	t.Helper()
	_, err := e.pool.Writer().Exec(context.Background(),
		`update orders_read set status = $2, updated_at = now() where order_id = $1`, orderID, status)
	require.NoError(t, err)
}

// sseEvent is one parsed SSE event.
type sseEvent struct {
	ID     int
	Status string
}

// openStream connects to the events endpoint and returns a channel of parsed
// events (closed when the stream ends) plus a cancel that tears the
// connection down.
func openStream(t *testing.T, httpc *http.Client, base, orderID string) (<-chan sseEvent, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/orders/"+orderID+"/events", http.NoBody)
	require.NoError(t, err)
	resp, err := httpc.Do(req) //nolint:bodyclose // closed by the reader goroutine below
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events := make(chan sseEvent, 32)
	go func() {
		defer close(events)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		var id int
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "id:"):
				id, _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "id:")))
			case strings.HasPrefix(line, "data:"):
				var payload struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload)
				events <- sseEvent{ID: id, Status: payload.Status}
			}
		}
	}()
	return events, cancel
}

// waitForStatus consumes events until want arrives, asserting ordinal
// monotonicity along the way (send() must never regress).
func waitForStatus(t *testing.T, events <-chan sseEvent, want string, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	lastID := -1
	for {
		select {
		case evt, ok := <-events:
			require.True(t, ok, "stream closed while waiting for %q", want)
			require.Greater(t, evt.ID, lastID, "event ids must be strictly increasing (no status regression)")
			lastID = evt.ID
			if evt.Status == want {
				return
			}
		case <-deadline:
			t.Fatalf("no %q event within %v", want, within)
		}
	}
}

// subscriberLive polls Redis until the streamer's shared broadcast
// subscription is established.
func subscriberLive(t *testing.T, tc rueidis.Client) {
	t.Helper()
	require.Eventually(t, func() bool {
		chans, err := tc.Do(context.Background(), tc.B().PubsubChannels().Build()).AsStrSlice()
		if err != nil {
			return false
		}
		for _, ch := range chans {
			if ch == "orders:status" {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "shared subscriber never subscribed to orders:status")
}

// TestStreamer_SharedSubscription_1500Streams proves the per-stream
// connection wall is gone: 1500 concurrent streams on ONE Streamer all
// receive a published status update while the Redis connection count stays
// flat (old design: 1 dedicated conn per stream → hard wall at 1024).
func TestStreamer_SharedSubscription_1500Streams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	addr := newRedisAddr(t)
	e := newEnv(t, dsn, addr)
	tc := newTestRedis(t, addr)

	orderID := uuid.NewString()
	e.seedOrder(t, orderID, "pending")

	const n = 1500
	tr := &http.Transport{MaxIdleConnsPerHost: n}
	defer tr.CloseIdleConnections()
	httpc := &http.Client{Transport: tr}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var connected atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for range n {
		g.Go(func() error {
			req, err := http.NewRequestWithContext(gctx, http.MethodGet,
				e.base+"/v1/orders/"+orderID+"/events", http.NoBody)
			if err != nil {
				return err
			}
			resp, err := httpc.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("status %d", resp.StatusCode)
			}
			sc := bufio.NewScanner(resp.Body)
			first := true
			for sc.Scan() {
				line := sc.Text()
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				if first {
					connected.Add(1)
					first = false
				}
				if strings.Contains(line, `"created"`) {
					return nil // got the broadcast update
				}
			}
			return fmt.Errorf("stream ended before created event: %w", sc.Err())
		})
	}

	// Wait until every stream is connected (received its snapshot event).
	require.Eventually(t, func() bool {
		return connected.Load() == n || gctx.Err() != nil
	}, 60*time.Second, 100*time.Millisecond, "not all streams connected")
	if gctx.Err() != nil {
		t.Fatalf("streams failed early: %v", g.Wait())
	}

	// With 1500 open streams the Redis connection count must stay flat:
	// 1 dedicated subscriber + a handful of pooled conns. NOT O(streams).
	list, err := tc.Do(ctx, tc.B().Arbitrary("CLIENT", "LIST").Build()).ToString()
	require.NoError(t, err)
	conns := strings.Count(strings.TrimSpace(list), "\n") + 1
	t.Logf("redis connections with %d open streams: %d", n, conns)
	require.Less(t, conns, 50, "redis connections must not scale with streams")

	// One status write + one Notify → every stream receives "created".
	e.setStatus(t, orderID, "created")
	e.streamer.Notify(ctx, orderID)
	require.NoError(t, g.Wait(), "every stream must receive the broadcast update")
}

// TestStreamer_MaxStreamsSaturation: the concurrent-stream bulkhead
// (WithMaxStreams) rejects the stream that exceeds the cap with 503
// GATEWAY_SSE_SATURATED + a Retry-After, while the held stream stays open. A
// cap of 1 proves the (cap+1)th rejection mechanism that the 4096 default
// relies on; the permit is acquired FIRST (before any DB read), an O(1)
// TryAcquirePermit, so a saturated replica sheds load at the door with no
// hot-path regression.
func TestStreamer_MaxStreamsSaturation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	// Poll mode (no Redis), cap = 1.
	e := newEnv(t, dsn, "", sse.WithMaxStreams(1))

	orderID := uuid.NewString()
	e.seedOrder(t, orderID, "pending")

	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	httpc := &http.Client{Transport: tr}

	// Hold the single permit with one long-lived stream.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		e.base+"/v1/orders/"+orderID+"/events", http.NoBody)
	require.NoError(t, err)
	held, err := httpc.Do(req) //nolint:bodyclose // closed via cancel + defer below
	require.NoError(t, err)
	defer held.Body.Close()
	require.Equal(t, http.StatusOK, held.StatusCode)

	// Drain at least the first event so the handler is past acquire and holding
	// the permit for the bulkhead's whole lifetime.
	require.Eventually(t, func() bool {
		sc := bufio.NewScanner(held.Body)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data:") {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "held stream never produced its snapshot")

	// The next stream exceeds the cap → 503 GATEWAY_SSE_SATURATED + Retry-After.
	require.Eventually(t, func() bool {
		req2, err := http.NewRequest(http.MethodGet, //nolint:noctx
			e.base+"/v1/orders/"+orderID+"/events", http.NoBody)
		if err != nil {
			return false
		}
		resp, err := httpc.Do(req2)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			return false
		}
		body, _ := io.ReadAll(resp.Body)
		require.NotEmpty(t, resp.Header.Get("Retry-After"), "saturated 503 must carry Retry-After")
		require.Contains(t, string(body), "GATEWAY_SSE_SATURATED")
		return true
	}, 10*time.Second, 100*time.Millisecond, "second stream must be rejected 503 while the cap is held")
}

// TestStreamer_GapRefresh_NoSilentStall: when the shared subscriber's
// connection dies AND a status change is written WITHOUT a publish
// (simulating a broadcast missed during the gap), the stream must still
// receive the new status — via the refresh-on-resubscribe path, NOT the slow
// safety poll (poll interval is set so the safety poll cannot fire within
// the assertion window).
func TestStreamer_GapRefresh_NoSilentStall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	addr := newRedisAddr(t)
	// pollInterval 1s → safety poll every 30s: anything observed within 15s
	// must have come from the resubscribe refresh.
	e := newEnv(t, dsn, addr, sse.WithPollInterval(time.Second))
	tc := newTestRedis(t, addr)

	orderID := uuid.NewString()
	e.seedOrder(t, orderID, "pending")

	events, _ := openStream(t, &http.Client{}, e.base, orderID)
	waitForStatus(t, events, "pending", 10*time.Second)
	subscriberLive(t, tc)

	// Status change committed WITHOUT a publish: the broadcast is "missed".
	e.setStatus(t, orderID, "paid")

	// Kill the subscriber's dedicated pub/sub connection → gap → the
	// streamer resubscribes and pushes a refresh to every registered stream.
	require.NoError(t, tc.Do(context.Background(),
		tc.B().Arbitrary("CLIENT", "KILL", "TYPE", "pubsub").Build()).Error())

	waitForStatus(t, events, "paid", 15*time.Second)
}

// TestStreamer_Coalescing: rapid created→paid publishes may skip the
// intermediate status but MUST end at paid and never regress (per-stream
// delivery coalesces to the highest ordinal; ids are strictly increasing).
func TestStreamer_Coalescing_RapidTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	addr := newRedisAddr(t)
	e := newEnv(t, dsn, addr)
	tc := newTestRedis(t, addr)

	orderID := uuid.NewString()
	e.seedOrder(t, orderID, "pending")

	events, _ := openStream(t, &http.Client{}, e.base, orderID)
	waitForStatus(t, events, "pending", 10*time.Second)
	subscriberLive(t, tc)

	ctx := context.Background()
	e.setStatus(t, orderID, "created")
	e.streamer.Notify(ctx, orderID)
	e.setStatus(t, orderID, "paid")
	e.streamer.Notify(ctx, orderID)

	// waitForStatus asserts strictly increasing ids on the way: the client
	// may see created then paid, or paid alone — never paid then created.
	waitForStatus(t, events, "paid", 15*time.Second)
}

// TestStreamer_Lifecycle_NoGoroutineLeaks churns streams open/closed, shuts
// the Streamer down with streams still open, and verifies no goroutines
// outlive the teardown.
func TestStreamer_Lifecycle_NoGoroutineLeaks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := pgtest.SharedDSN(t)
	addr := newRedisAddr(t)
	// Snapshot AFTER container infra is up so its goroutines are excluded.
	defer goleak.VerifyNone(t, goleakopts.Default(goleak.IgnoreCurrent())...)

	e := newEnv(t, dsn, addr)
	orderID := uuid.NewString()
	e.seedOrder(t, orderID, "pending")

	httpc := &http.Client{Transport: &http.Transport{}}

	// Churn: open streams, read the snapshot, close them client-side.
	for range 10 {
		events, cancelStream := openStream(t, httpc, e.base, orderID)
		waitForStatus(t, events, "pending", 10*time.Second)
		cancelStream()
		drain(t, events, 10*time.Second)
	}

	// Leave streams open across Shutdown: the server must end them.
	open := make([]<-chan sseEvent, 0, 5)
	for range 5 {
		events, _ := openStream(t, httpc, e.base, orderID)
		waitForStatus(t, events, "pending", 10*time.Second)
		open = append(open, events)
	}

	e.close() // Shutdown (ends streams + subscriber), then srv/client/pool
	for _, events := range open {
		drain(t, events, 10*time.Second)
	}
	httpc.CloseIdleConnections()
}

// drain consumes events until the stream closes.
func drain(t *testing.T, events <-chan sseEvent, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream did not close in time")
		}
	}
}
