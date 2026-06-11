package cache

// Internal (white-box) tests for the L2 error visibility seam (l2Error):
// the cache.l2.errors counter and the WARN log replace the historical
// silent-ignore of L2 failures. No Redis needed.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"go-boilerplate/platform/observability/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// withManualMeter swaps the global meter provider for a manual-reader one for
// the duration of the test, returning the reader to collect from.
func withManualMeter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

// sumByOp collects the cache.l2.errors counter and returns value-by-op.
func sumByOp(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "cache.l2.errors" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "cache.l2.errors must be an int64 sum, got %T", m.Data)
			for _, dp := range sum.DataPoints {
				for _, attr := range dp.Attributes.ToSlice() {
					if string(attr.Key) == "op" {
						out[attr.Value.AsString()] += dp.Value
					}
				}
			}
		}
	}
	return out
}

// TestL2Error_CounterAndWarn verifies the visibility contract: every recorded
// L2 error increments cache.l2.errors{op} and emits a WARN with the op, key
// and error on the ctx logger.
func TestL2Error_CounterAndWarn(t *testing.T) {
	reader := withManualMeter(t)
	c := &Cache{l2errs: newL2ErrorCounter()}

	var buf bytes.Buffer
	ctx := log.Into(context.Background(), slog.New(slog.NewJSONHandler(&buf, nil)))

	c.l2Error(ctx, "get", "k1", errors.New("boom"))
	c.l2Error(ctx, "set", "k1", errors.New("boom"))
	c.l2Error(ctx, "set", "k2", errors.New("boom"))
	c.l2Error(ctx, "del", "k1", errors.New("boom"))

	byOp := sumByOp(t, reader)
	assert.Equal(t, int64(1), byOp["get"])
	assert.Equal(t, int64(2), byOp["set"])
	assert.Equal(t, int64(1), byOp["del"])

	logged := buf.String()
	assert.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(t, logged, "cache: L2 error")
	assert.Contains(t, logged, `"op":"get"`)
	assert.Contains(t, logged, `"key":"k1"`)
	assert.Contains(t, logged, "boom")
}

// TestL2Error_CallerCancelledNotRecorded mirrors the breaker's failure
// attribution: an op that failed because the CALLER's ctx ended says nothing
// about Redis health — neither the counter nor the WARN fires, so a burst of
// cancelled requests cannot masquerade as an L2 outage in dashboards.
func TestL2Error_CallerCancelledNotRecorded(t *testing.T) {
	reader := withManualMeter(t)
	c := &Cache{l2errs: newL2ErrorCounter()}

	var buf bytes.Buffer
	cancelled, cancel := context.WithCancel(
		log.Into(context.Background(), slog.New(slog.NewJSONHandler(&buf, nil))),
	)
	cancel()

	c.l2Error(cancelled, "get", "k1", context.Canceled)

	byOp := sumByOp(t, reader)
	assert.Empty(t, byOp, "caller-cancelled ops must not count as L2 errors")
	assert.Empty(t, buf.String(), "caller-cancelled ops must not WARN")
}

// TestL2Error_NilCounterStillWarns pins the nil-degrade contract: metric
// creation failure must never silence the log path.
func TestL2Error_NilCounterStillWarns(t *testing.T) {
	c := &Cache{} // l2errs nil

	var buf bytes.Buffer
	ctx := log.Into(context.Background(), slog.New(slog.NewJSONHandler(&buf, nil)))

	c.l2Error(ctx, "set", "k", errors.New("boom"))
	assert.Contains(t, buf.String(), "cache: L2 error")
}
