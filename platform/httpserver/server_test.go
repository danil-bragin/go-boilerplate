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
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + srv.Addr() + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, "pong", string(body))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}
