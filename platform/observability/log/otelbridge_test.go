package log_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"go-boilerplate/platform/observability/log"

	"github.com/stretchr/testify/require"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
)

// memExporter is an in-memory sdklog.Exporter capturing every record.
type memExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *memExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}

func (e *memExporter) Shutdown(context.Context) error   { return nil }
func (e *memExporter) ForceFlush(context.Context) error { return nil }

func (e *memExporter) snapshot() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

func TestWithOTelBridge_FansOutWithTraceCorrelation(t *testing.T) {
	exp := &memExporter{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf, log.WithOTelBridge(provider))

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "bridged", "k", "v")

	// Stdout sink still got the record.
	require.Contains(t, buf.String(), "bridged")

	// Bridge exported the record with native trace correlation from ctx.
	records := exp.snapshot()
	require.Len(t, records, 1)
	require.Equal(t, "bridged", records[0].Body().AsString())
	require.Equal(t, sc.TraceID(), records[0].TraceID())
	require.Equal(t, sc.SpanID(), records[0].SpanID())
}

func TestWithOTelBridge_LevelGatesBothSinks(t *testing.T) {
	exp := &memExporter{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "warn", Format: "json"}, &buf, log.WithOTelBridge(provider))

	logger.InfoContext(context.Background(), "below-threshold")

	require.Empty(t, exp.snapshot(), "info record must not reach the bridge at warn level")
	require.NotContains(t, buf.String(), "below-threshold")
}

func TestWithOTelBridge_NilProviderIsNoop(t *testing.T) {
	var buf bytes.Buffer
	// Wiring a nil provider through must behave exactly like plain New —
	// servicekit passes telemetry.Providers.LoggerProvider unconditionally.
	logger, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf, log.WithOTelBridge(nil))

	logger.InfoContext(context.Background(), "stdout-only")

	require.True(t, strings.Contains(buf.String(), "stdout-only"))
}
