package main

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestApp_StartHealthAndStop(t *testing.T) {
	defer goleak.VerifyNone(
		t,
		goleak.IgnoreTopFunction("go.opentelemetry.io/otel/sdk/trace.(*batchSpanProcessor).processQueue"),
	)

	t.Setenv("HTTP_ADDR", ":0")                // use a random free port
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0") // harness admin server: ephemeral port
	t.Setenv("DRAIN_GRACE", "0")
	t.Setenv("OTEL_ENABLED", "false")

	app, err := newApp(context.Background())
	require.NoError(t, err)
	require.NoError(t, app.start())

	resp, err := http.Get("http://" + app.server.Addr() + "/livez")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", string(body))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, app.stop(ctx))
}
