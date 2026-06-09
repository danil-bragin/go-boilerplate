package outbox

import (
	"context"
	"embed"
	"errors"
	"sync"
	"testing"

	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

//go:embed migrations/*.sql
var migrationsInternal embed.FS

var (
	readerOnce sync.Once
	reader     *sdkmetric.ManualReader
)

func manualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	readerOnce.Do(func() {
		reader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	})
	return reader
}

func sumCounter(rm *metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func gaugeValue(rm *metricdata.ResourceMetrics, name string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) > 0 {
				return g.DataPoints[len(g.DataPoints)-1].Value, true
			}
		}
	}
	return 0, false
}

func newMetricsPool(t *testing.T) *pg.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrationsInternal, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

func insertUnpublished(t *testing.T, pool *pg.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		_, err := pool.Writer().Exec(ctx,
			`insert into outbox (id, aggregate_type, aggregate_id, event_type, payload, headers)
			 values ($1, 'order', 'x', 'Test', '{}', '{}')`, uuid.New())
		require.NoError(t, err)
	}
}

type countingPub struct{ fail bool }

func (p *countingPub) Publish(_ context.Context, _ Message) error {
	if p.fail {
		return errors.New("countingPub: publish failed")
	}
	return nil
}

// TestRelayMetrics_PendingPublishedErrors verifies the relay instruments:
// outbox.pending gauge (count of unpublished rows), outbox.published counter
// on success, outbox.publish_errors counter on publish failure.
func TestRelayMetrics_PendingPublishedErrors(t *testing.T) {
	rd := manualReader(t)
	pool := newMetricsPool(t)
	ctx := context.Background()

	relay := NewRelay(pool, &countingPub{}, RelayConfig{BatchSize: 10})
	insertUnpublished(t, pool, 3)

	// Pending gauge reflects the unpublished backlog.
	relay.reportPending(ctx)
	var rm metricdata.ResourceMetrics
	require.NoError(t, rd.Collect(ctx, &rm))
	pending, ok := gaugeValue(&rm, "outbox.pending")
	require.True(t, ok, "outbox.pending gauge must be reported")
	require.Equal(t, int64(3), pending)

	// Successful batch moves outbox.published by 3.
	n, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.NoError(t, rd.Collect(ctx, &rm))
	require.Equal(t, int64(3), sumCounter(&rm, "outbox.published"))

	// Pending drops to zero after publishing.
	relay.reportPending(ctx)
	require.NoError(t, rd.Collect(ctx, &rm))
	pending, ok = gaugeValue(&rm, "outbox.pending")
	require.True(t, ok)
	require.Equal(t, int64(0), pending)

	// Publish failure moves outbox.publish_errors.
	insertUnpublished(t, pool, 1)
	failing := NewRelay(pool, &countingPub{fail: true}, RelayConfig{BatchSize: 10})
	_, err = failing.ProcessBatch(ctx)
	require.Error(t, err)
	require.NoError(t, rd.Collect(ctx, &rm))
	require.GreaterOrEqual(t, sumCounter(&rm, "outbox.publish_errors"), int64(1))
}
