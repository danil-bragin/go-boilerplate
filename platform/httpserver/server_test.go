package httpserver_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpserver"
)

func TestServer_ServesAndShutsDown(t *testing.T) {
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
	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())

	err := srv.Start()
	require.Error(t, err, "second Start() must return an error")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
