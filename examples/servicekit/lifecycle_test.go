package servicekit

// Internal (white-box) lifecycle tests: these verify the exact teardown order
// of the harness — drain-gate (readyz→503 + grace) FIRST, consumers-cancel
// second, mid-stack resources after that, and the admin server LAST so /readyz
// keeps answering 503 for the entire drain window.
//
// The tests are integration tests (they need Postgres + Redpanda containers
// because servicekit.New connects to both) and are skipped in -short mode.

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readyzStatus GETs /readyz on the admin server and returns the HTTP status,
// or 0 when the connection failed (server down).
func readyzStatus(addr string) int {
	resp, err := http.Get("http://" + addr + "/readyz") //nolint:noctx
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// eventLog records named teardown events in order.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	copy(out, l.events)
	return out
}

func newLifecycleService(t *testing.T, drainGrace time.Duration) *Service {
	t.Helper()

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	cfg := Config{AdminAddr: "127.0.0.1:0"}
	cfg.PG.DSN = dsn
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	cfg.InboxCleanupInterval = 0
	cfg.DrainGrace = drainGrace

	svc, err := New(context.Background(), cfg, nil, "")
	require.NoError(t, err)
	return svc
}

// TestStop_TeardownOrder asserts the closer sequence with instrumented
// (fake) closers and a fake consumer goroutine:
//
//  1. drain-gate fires first: /readyz flips to 503 before anything else.
//  2. consumers-cancel fires second: the consumer goroutine observes ctx
//     cancellation only AFTER readiness already reads 503.
//  3. mid-stack closers (registered between New and Start) run after the
//     consumers have fully stopped, with the admin server STILL serving 503.
//  4. the admin server dies last: after Stop returns, /readyz is unreachable.
func TestStop_TeardownOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const grace = 300 * time.Millisecond
	svc := newLifecycleService(t, grace)
	log := &eventLog{}

	// Fake closer registered right after New: in LIFO order it runs after
	// consumers-cancel and before the admin server teardown.
	svc.closer.Add("test-mid-teardown", func(context.Context) error {
		if readyzStatus(svc.AdminAddr()) == http.StatusServiceUnavailable {
			log.add("mid-teardown:readyz-503")
		} else {
			log.add("mid-teardown:readyz-up")
		}
		return nil
	})

	// Fake consumer goroutine: records whether readiness had already flipped
	// when its context was cancelled.
	consumerCancelled := make(chan struct{})
	svc.goroutines = append(svc.goroutines, func(ctx context.Context) {
		<-ctx.Done()
		if readyzStatus(svc.AdminAddr()) == http.StatusServiceUnavailable {
			log.add("consumer-cancelled:readyz-503")
		} else {
			log.add("consumer-cancelled:readyz-up")
		}
		close(consumerCancelled)
	})

	require.NoError(t, svc.Start())

	// Fake closer registered after Start: in LIFO order it is the very first
	// teardown step (before even the drain-gate) — readiness must still be 200.
	svc.closer.Add("test-pre-drain", func(context.Context) error {
		if readyzStatus(svc.AdminAddr()) == http.StatusOK {
			log.add("pre-drain:readyz-200")
		} else {
			log.add("pre-drain:readyz-not-ok")
		}
		return nil
	})

	// Wait for the admin server to accept connections.
	require.Eventually(t, func() bool {
		return readyzStatus(svc.AdminAddr()) == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond, "admin server did not come up")

	start := time.Now()
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))
	elapsed := time.Since(start)

	select {
	case <-consumerCancelled:
	default:
		t.Fatal("consumer goroutine was never cancelled")
	}

	assert.Equal(t, []string{
		"pre-drain:readyz-200",
		"consumer-cancelled:readyz-503",
		"mid-teardown:readyz-503",
	}, log.list(), "teardown order must be: drain-gate → consumers-cancel → mid closers → admin last")

	assert.GreaterOrEqual(t, elapsed, grace, "Stop must hold the DRAIN_GRACE window")
	assert.Equal(t, 0, readyzStatus(svc.AdminAddr()), "admin server must be down after Stop returns")
}

// TestStop_ReadyzServes503DuringDrain polls /readyz from the outside while
// Stop runs with a non-zero grace period: the endpoint must answer 503 (not
// connection-refused) during the drain window.
func TestStop_ReadyzServes503DuringDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const grace = 500 * time.Millisecond
	svc := newLifecycleService(t, grace)
	require.NoError(t, svc.Start())

	require.Eventually(t, func() bool {
		return readyzStatus(svc.AdminAddr()) == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond, "admin server did not come up")

	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		stopDone <- svc.Stop(stopCtx)
	}()

	// During the drain window /readyz must flip to 503 while remaining
	// reachable (admin server is the LAST thing to shut down).
	saw503 := false
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if readyzStatus(svc.AdminAddr()) == http.StatusServiceUnavailable {
			saw503 = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.True(t, saw503, "/readyz must serve 503 while Stop is draining")

	require.NoError(t, <-stopDone)
}
