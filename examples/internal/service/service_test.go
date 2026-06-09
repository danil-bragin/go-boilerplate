package service_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"go-boilerplate/examples/internal/service"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestService_AdminEndpoints starts a Service against real Redpanda + Postgres
// (via testcontainers helpers) and asserts that /livez, /readyz, and /metrics
// respond 200 before Stop cleans up without error.
func TestService_AdminEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	cfg := service.Config{
		AdminAddr: "127.0.0.1:0", // random port avoids conflicts
	}
	cfg.PG.DSN = dsn
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Telemetry.MetricsPrometheus = true
	cfg.Log.Level = "error"

	ctx := context.Background()
	svc, err := service.New(ctx, cfg, nil, "")
	require.NoError(t, err)

	err = svc.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Stop(stopCtx)
	})

	adminURL := "http://" + svc.AdminAddr()

	// Poll until the server is accepting connections (up to 5s).
	var (
		resp    *http.Response
		connErr error
	)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, connErr = http.Get(adminURL + "/livez") //nolint:noctx
		if connErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, connErr, "admin server did not come up in time")
	defer resp.Body.Close()

	// /livez → 200
	assert.Equal(t, http.StatusOK, resp.StatusCode, "GET /livez")
	_, _ = io.Copy(io.Discard, resp.Body)

	// /readyz → 200 (pg + kafka healthy)
	resp2, err := http.Get(adminURL + "/readyz") //nolint:noctx
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode, "GET /readyz")
	_, _ = io.Copy(io.Discard, resp2.Body)

	// /metrics → 200 with Prometheus text
	resp3, err := http.Get(adminURL + "/metrics") //nolint:noctx
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode, "GET /metrics")
	body, _ := io.ReadAll(resp3.Body)
	assert.Contains(t, string(body), "# HELP", "expected Prometheus exposition format")
}
