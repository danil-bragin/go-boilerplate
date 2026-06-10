package pg_test

import (
	"context"
	"sync"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricsReader installs a manual-reader SDK provider as the otel global,
// exactly once per test binary (instruments created from the global meter
// bind to the first provider set).
var (
	metricsReaderOnce sync.Once
	metricsReader     *sdkmetric.ManualReader
)

func manualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	metricsReaderOnce.Do(func() {
		metricsReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricsReader)))
	})
	return metricsReader
}

// queryDurationCount sums pg.query.duration datapoint counts matching the
// given {query, pool} attribute pair.
func queryDurationCount(rm *metricdata.ResourceMetrics, query, pool string) uint64 {
	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "pg.query.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				attrs := map[string]string{}
				for _, kv := range dp.Attributes.ToSlice() {
					attrs[string(kv.Key)] = kv.Value.AsString()
				}
				if attrs["query"] == query && attrs["pool"] == pool {
					total += dp.Count
				}
			}
		}
	}
	return total
}

// TestQueryTracer_RecordsDurationPerQueryAndPool (integration): with
// PG_QUERY_METRICS enabled, every query records a pg.query.duration sample
// labeled with the sqlc query name (or "raw") and the pool role
// (writer|reader); with it disabled, nothing is recorded.
func TestQueryTracer_RecordsDurationPerQueryAndPool(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	reader := manualReader(t)
	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()

	pool, err := pg.New(ctx, pg.Config{
		DSN:          config.Secret(dsn),
		ReaderDSN:    config.Secret(dsn), // same instance, distinct pool → pool="reader"
		MaxConns:     5,
		MinConns:     1,
		QueryMetrics: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	// sqlc-named query on the WRITER pool.
	var one int
	require.NoError(t, pool.Writer().
		QueryRow(ctx, "-- name: TracerProbe :one\nselect 1").Scan(&one))

	// Raw (un-named) query on the READER pool.
	require.NoError(t, pool.Reader().QueryRow(ctx, "select 2").Scan(&one))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	require.GreaterOrEqual(t, queryDurationCount(&rm, "TracerProbe", "writer"), uint64(1),
		"sqlc-named writer query must be sampled as {query=TracerProbe, pool=writer}")
	require.GreaterOrEqual(t, queryDurationCount(&rm, "raw", "reader"), uint64(1),
		"un-named reader query must fall back to {query=raw, pool=reader}")

	// Disabled pool: no tracer, no samples for a fresh unique query name.
	off, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn), MaxConns: 2, MinConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = off.Close(ctx) })
	require.NoError(t, off.Writer().
		QueryRow(ctx, "-- name: TracerDisabledProbe :one\nselect 3").Scan(&one))

	require.NoError(t, reader.Collect(ctx, &rm))
	require.Zero(t, queryDurationCount(&rm, "TracerDisabledProbe", "writer"),
		"QueryMetrics=false must not install the tracer")
}
