package servicekit

// White-box teardown-order test for AddHTTPServer. Runs in -short mode: with
// WithoutKafka+WithoutPG no container is needed, so the closer ordering of the
// public HTTP server can be asserted cheaply.

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"go-boilerplate/platform/web/httpserver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddHTTPServer_TeardownOrder asserts the public-server slot in teardown:
//
//	drain-gate (readyz→503) → public HTTP shutdown → consumers-cancel → … → admin last.
//
// The fake consumer goroutine observes, at the moment its context is
// cancelled, that (a) readiness already reads 503 and (b) the public server
// is already down — i.e. the public server shut AFTER the drain-gate flipped
// readiness and BEFORE consumers were cancelled (and therefore before the
// kafka/pg closers, which run even later).
func TestAddHTTPServer_TeardownOrder(t *testing.T) {
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	cfg.DrainGrace = 50 * time.Millisecond

	svc, err := New(context.Background(), cfg, nil, "", WithoutKafka(), WithoutPG())
	require.NoError(t, err)

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	srv.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	svc.AddHTTPServer("public", srv)

	log := &eventLog{}
	svc.goroutines = append(svc.goroutines, func(ctx context.Context) {
		<-ctx.Done()
		if readyzStatus(svc.AdminAddr()) == http.StatusServiceUnavailable {
			log.add("consumer-cancelled:readyz-503")
		} else {
			log.add("consumer-cancelled:readyz-up")
		}
		if _, pingErr := http.Get("http://" + srv.Addr() + "/ping"); pingErr != nil { //nolint:noctx,bodyclose
			log.add("consumer-cancelled:public-down")
		} else {
			log.add("consumer-cancelled:public-up")
		}
	})

	require.NoError(t, svc.Start())
	require.Eventually(t, func() bool {
		return readyzStatus(svc.AdminAddr()) == http.StatusOK
	}, 5*time.Second, 25*time.Millisecond, "admin server did not come up")

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))

	assert.Equal(t, []string{
		"consumer-cancelled:readyz-503",
		"consumer-cancelled:public-down",
	}, log.list(), "public server must shut after drain-gate and before consumers-cancel")
}

// TestAddHTTPServer_PartialStartFailureShutsStartedServers: when server N
// fails to bind, servers 0..N-1 already started MUST have their shutdown
// closers registered, so the teardown that Main runs after a failed Start
// actually shuts them down (no orphaned listeners on a half-started pod).
func TestAddHTTPServer_PartialStartFailureShutsStartedServers(t *testing.T) {
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	cfg.DrainGrace = 0

	svc, err := New(context.Background(), cfg, nil, "", WithoutKafka(), WithoutPG())
	require.NoError(t, err)

	// Occupy a port so the second server's bind fails.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	first := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	first.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	second := httpserver.New(httpserver.Config{Addr: ln.Addr().String()}) // bind will fail

	svc.AddHTTPServer("first", first)
	svc.AddHTTPServer("second", second)

	err = svc.Start()
	require.Error(t, err, "second server's bind failure must fail Start")
	require.Contains(t, err.Error(), "second")

	// The first server bound successfully and is still serving at this point.
	resp, err := http.Get("http://" + first.Addr() + "/ping") //nolint:noctx
	require.NoError(t, err, "first server must be up after the partial start")
	_ = resp.Body.Close()

	// Teardown via the closer (what servicekit.Main does after a failed
	// Start) must shut the first server down.
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))

	_, err = http.Get("http://" + first.Addr() + "/ping") //nolint:noctx,bodyclose
	require.Error(t, err, "first server must be shut down by the closer after a partial-start failure")
}
