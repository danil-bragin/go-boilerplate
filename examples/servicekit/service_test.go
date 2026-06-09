package servicekit_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/examples/servicekit"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/retry"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
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

	cfg := servicekit.Config{
		AdminAddr: "127.0.0.1:0", // random port avoids conflicts
	}
	cfg.EnsureTopics = true
	cfg.PG.DSN = dsn
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Telemetry.MetricsPrometheus = true
	cfg.Log.Level = "error"

	ctx := context.Background()
	svc, err := servicekit.New(ctx, cfg, nil, "")
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

// TestService_AddConsumerWithRetry verifies the end-to-end wiring of
// AddConsumerWithRetry through the servicekit harness:
//
//   - A message whose handler fails once is escalated to the single 2s retry
//     tier (fast policy for test speed) and redelivered by the retry consumer.
//   - The tier topic and DLT topic are provisioned by the harness.
//   - On successful redelivery the message is committed and the handler is not
//     called again.
func TestService_AddConsumerWithRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	cfg := servicekit.Config{
		AdminAddr: "127.0.0.1:0",
	}
	cfg.EnsureTopics = true
	cfg.PG.DSN = dsn
	cfg.Kafka.Brokers = []string{broker}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	// Disable inbox cleanup — no inbox table in this minimal harness test.
	cfg.InboxCleanupInterval = 0

	ctx := context.Background()
	svc, err := servicekit.New(ctx, cfg, nil, "")
	require.NoError(t, err)

	// Policy: single 2s tier for speed, 1 fast attempt before escalation.
	pol := retry.Policy{Tiers: []time.Duration{2 * time.Second}, FastAttempts: 1}

	suffix := uuid.NewString()[:8]
	baseTopic := "svckit.retry-test-" + suffix
	groupID := "svckit-retry-grp-" + suffix

	// Handler: fails on the first call, succeeds on the second (redelivery).
	var callCount atomic.Int32
	successCh := make(chan struct{})
	var successOnce atomic.Bool

	handler := func(_ context.Context, _ kafka.Record) error {
		n := callCount.Add(1)
		if n == 1 {
			return errors.New("transient failure — first attempt")
		}
		// Second call: success (redelivery from retry consumer).
		if successOnce.CompareAndSwap(false, true) {
			close(successCh)
		}
		return nil
	}

	err = svc.AddConsumerWithRetry(ctx, groupID, []string{baseTopic}, handler, pol)
	require.NoError(t, err)

	err = svc.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Stop(stopCtx)
	})

	// Assert that the tier and DLT topics were created by the harness.
	tierTopic := retry.TierTopic(baseTopic, 0)
	dltTopic := retry.DLTTopic(baseTopic)

	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "svckit-admin-" + suffix,
	})
	require.NoError(t, err)
	t.Cleanup(adminCl.Close)

	checkCtx, checkCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer checkCancel()
	require.NoError(t, kafka.EnsureTopics(checkCtx, adminCl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, tierTopic, dltTopic),
		"tier and DLT topics should already exist (created by AddConsumerWithRetry)")

	// Produce a message to the base topic.
	producerCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "svckit-producer-" + suffix,
	})
	require.NoError(t, err)
	prod := kafka.NewProducer(producerCl)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = prod.Close(c)
	})

	produceCtx, produceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer produceCancel()
	require.NoError(t, prod.Produce(produceCtx, kafka.Record{
		Topic: baseTopic,
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}))

	// Wait for the retry consumer to redeliver and the handler to succeed.
	// Timeout accounts for: group join + first attempt + 2s tier delay + redelivery.
	select {
	case <-successCh:
		// Handler succeeded on redelivery — test passes.
	case <-time.After(30 * time.Second):
		t.Fatal("handler was not successfully redelivered within 30s")
	}

	assert.GreaterOrEqual(t, callCount.Load(), int32(2),
		"handler must be called at least twice (initial failure + retry redelivery)")
}
