package httpserver_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/web/httpserver"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestServer_ServesAndShutsDown(t *testing.T) {
	// Serve goroutine must exit with Shutdown. Deferred LIFO: idle client
	// conns are dropped first, then the leak check runs.
	defer goleak.VerifyNone(t)
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	srv.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	require.NoError(t, srv.Start())

	resp, err := http.Get("http://" + srv.Addr() + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, "pong", string(body))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}

// A3 – Notify() must unblock on graceful shutdown
func TestNotify_UnblocksOnGracefulShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())

	done := make(chan struct{})
	go func() {
		<-srv.Notify() // blocks until shutdown closes the channel
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case <-done:
		// OK – channel was closed by Shutdown
	case <-time.After(time.Second):
		t.Fatal("Notify() did not unblock within 1s after Shutdown")
	}
}

// A6 – Start() twice must error
func TestStart_TwiceErrors(t *testing.T) {
	defer goleak.VerifyNone(t)

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())

	err := srv.Start()
	require.Error(t, err, "second Start() must return an error")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// TestShutdown_DoubleConcurrentShutdownNoPanic verifies that calling Shutdown
// from two goroutines simultaneously does not panic (no double-close of the
// serveErr channel). The mutex + closed flag must absorb both calls safely.
func TestShutdown_DoubleConcurrentShutdownNoPanic(t *testing.T) {
	defer goleak.VerifyNone(t)

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			// Either call may return a non-nil error from http.Server.Shutdown
			// (the second caller sees ErrServerClosed), but neither must panic.
			_ = srv.Shutdown(ctx)
		}()
	}

	require.NotPanics(t, func() { wg.Wait() })
}

// TestStart_WarnsWhenNoServerWideMaxBytes: a server built WithoutMaxBytes
// has no server-wide request-body cap — Start must emit a WARN reminding the
// operator that every route group needs its own httpserver.MaxBytes.
func TestStart_WarnsWhenNoServerWideMaxBytes(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"}, httpserver.WithoutMaxBytes())
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	require.Contains(t, buf.String(), "WithoutMaxBytes",
		"Start must warn when no server-wide request-body cap is installed")
}

// TestStart_NoWarnWithServerWideMaxBytes: the default stack (server-wide
// MaxBytes installed) must not warn.
func TestStart_NoWarnWithServerWideMaxBytes(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	require.NotContains(t, buf.String(), "WithoutMaxBytes")
}
