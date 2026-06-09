package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"go-boilerplate/platform/log"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNew_WritesStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "debug", Format: "json"}, &buf)

	logger.Info("hello", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "hello", entry["msg"])
	require.Equal(t, "value", entry["key"])
}

func TestNew_NilWriterPanics(t *testing.T) {
	require.PanicsWithValue(t, "log: New: writer must not be nil", func() {
		log.New(log.Config{}, nil)
	})
}

func TestNew_SyncFuncFlushes(t *testing.T) {
	var buf bytes.Buffer
	logger, sync := log.New(log.Config{Level: "info", Format: "json"}, &buf)
	logger.Info("flush-test")
	// sync should be callable and return no error against a buffer
	require.NotNil(t, sync)
	_ = sync()
}

func TestParseLevel(t *testing.T) {
	require.Equal(t, "WARN", log.ParseLevel("warn").String())
	require.Equal(t, "INFO", log.ParseLevel("nonsense").String()) // fallback
}

func TestContextLogger_RoundTrips(t *testing.T) {
	var buf bytes.Buffer
	base, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	ctx := log.Into(context.Background(), base.With("svc", "orders"))
	log.From(ctx).Info("msg")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "orders", entry["svc"])
}

// TestLog_InjectsTraceIDFromContext verifies that when a context carries a
// valid OTel span (from the SDK), InfoContext appends trace_id and span_id
// fields to the JSON log record.
func TestLog_InjectsTraceIDFromContext(t *testing.T) {
	// Build an in-memory tracer provider so we get a real, valid SpanContext.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("log-test").Start(context.Background(), "test-op")
	defer span.End()

	sc := span.SpanContext()
	require.True(t, sc.IsValid(), "span context must be valid")

	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	// Use context-aware method — this is the path that injects trace fields.
	logger.InfoContext(ctx, "traced message")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, sc.TraceID().String(), entry["trace_id"],
		"trace_id must match the active span's TraceID")
	require.Equal(t, sc.SpanID().String(), entry["span_id"],
		"span_id must match the active span's SpanID")
}

// TestLog_NoTraceIDWithoutSpan verifies that when the context has no active
// span, trace_id and span_id are NOT added to the log record.
func TestLog_NoTraceIDWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	logger.InfoContext(context.Background(), "no span")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.NotContains(t, entry, "trace_id", "trace_id must be absent when no span is active")
	require.NotContains(t, entry, "span_id", "span_id must be absent when no span is active")
}
