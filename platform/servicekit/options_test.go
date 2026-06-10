package servicekit_test

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/retry"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/web/httpserver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// optsConfig returns a config pointing at UNREACHABLE pg/kafka endpoints.
// Tests that pass WithoutKafka/WithoutPG prove that New performs NO dial:
// construction must succeed fast even though nothing is listening.
func optsConfig() servicekit.Config {
	cfg := servicekit.Config{AdminAddr: "127.0.0.1:0"}
	cfg.PG.DSN = "postgres://nouser:nopass@127.0.0.1:1/nodb?connect_timeout=1"
	cfg.Kafka.Brokers = []string{"127.0.0.1:1"}
	cfg.Telemetry.Enabled = false
	cfg.Log.Level = "error"
	cfg.DrainGrace = 0
	cfg.EnsureTopics = true // so EnsureTopics is NOT a config no-op in these tests
	return cfg
}

// TestNew_WithoutKafkaAndPG proves a service can be built with neither Kafka
// nor Postgres: New must not dial anything (the configured endpoints are
// unreachable), the kafka/pg-dependent adders must return clear errors, and
// the admin lifecycle (Start/livez/Stop) must work.
func TestNew_WithoutKafkaAndPG(t *testing.T) {
	cfg := optsConfig()

	start := time.Now()
	svc, err := servicekit.New(context.Background(), cfg, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	require.NoError(t, err, "New must succeed without dialing kafka/pg")
	require.Less(t, time.Since(start), 5*time.Second, "New must not block on unreachable endpoints")

	assert.Nil(t, svc.Pool(), "WithoutPG must leave the pool nil")
	assert.Nil(t, svc.Producer(), "WithoutKafka must leave the producer nil")
	assert.Nil(t, svc.KafkaClient(), "WithoutKafka must leave the kafka client nil")

	ctx := context.Background()

	// Kafka-dependent adders must fail with a clear error.
	err = svc.AddConsumer(ctx, "g", []string{"t"}, func(context.Context, kafka.Record) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutKafka")

	err = svc.AddConsumerWithRetry(ctx, "g", []string{"t"},
		func(context.Context, kafka.Record) error { return nil }, retry.DefaultPolicy())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutKafka")

	err = svc.EnsureTopics(ctx, "t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutKafka")

	// PG-dependent adders must fail with a clear error.
	err = svc.AddOutboxRelay(nil, outbox.RelayConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutPG")

	// Lifecycle still works: admin server runs, /livez answers, Stop is clean.
	require.NoError(t, svc.Start())
	resp, err := http.Get("http://" + svc.AdminAddr() + "/livez") //nolint:noctx
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))
}

// TestNew_WithoutPG proves WithoutPG alone skips the pool and migrations while
// keeping kafka wiring (kgo construction is offline, so no broker is needed).
func TestNew_WithoutPG(t *testing.T) {
	cfg := optsConfig()

	svc, err := servicekit.New(context.Background(), cfg, nil, "", servicekit.WithoutPG())
	require.NoError(t, err, "New with WithoutPG must not dial postgres")

	assert.Nil(t, svc.Pool())
	assert.NotNil(t, svc.Producer(), "kafka stays wired without PG")
	assert.NotNil(t, svc.KafkaClient())

	err = svc.AddOutboxRelay(svc.DefaultOutboxPublisher(), outbox.RelayConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutPG")

	require.NoError(t, svc.Start())
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))
}

// TestAddWorker_RunsAndStops: AddWorker goroutines start with Start and are
// cancelled + awaited during Stop.
func TestAddWorker_RunsAndStops(t *testing.T) {
	cfg := optsConfig()
	svc, err := servicekit.New(context.Background(), cfg, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	require.NoError(t, err)

	started := make(chan struct{})
	var stopped atomic.Bool
	require.NoError(t, svc.AddWorker("test-worker", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		stopped.Store(true)
	}))

	require.NoError(t, svc.Start())
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))
	assert.True(t, stopped.Load(), "Stop must cancel and WAIT for the worker to exit")
}

// TestAddHTTPServer_Lifecycle: a public HTTP server registered via
// AddHTTPServer is started by Start and gracefully shut down by Stop.
func TestAddHTTPServer_Lifecycle(t *testing.T) {
	cfg := optsConfig()
	svc, err := servicekit.New(context.Background(), cfg, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	require.NoError(t, err)

	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	srv.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	svc.AddHTTPServer("public", srv)

	require.NoError(t, svc.Start(), "Start must bind the public server")

	resp, err := http.Get("http://" + srv.Addr() + "/ping") //nolint:noctx
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))

	_, err = http.Get("http://" + srv.Addr() + "/ping") //nolint:noctx,bodyclose
	assert.Error(t, err, "public server must be down after Stop")
}
