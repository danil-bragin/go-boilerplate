package cqrs_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/observability/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// withTestTracer installs an in-memory SDK tracer provider as the global
// provider for the duration of the test.
func withTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return exp
}

// TestLogging_CarriesTraceID: with the REQUIRED pipeline order — Tracing
// OUTERMOST, Logging inside it — every log record emitted by the Logging
// behavior must carry trace_id/span_id fields (the platform/log traceHandler
// injects them from the ctx span). This is the trace↔log correlation
// contract.
func TestLogging_CarriesTraceID(t *testing.T) {
	exp := withTestTracer(t)

	var buf bytes.Buffer
	logger, sync := log.New(log.Config{Level: "debug", Format: "json"}, &buf)
	t.Cleanup(func() { _ = sync() })

	handler := func(_ context.Context, _ string) (string, error) { return "ok", nil }
	decorated := cqrs.Decorate(
		handler,
		cqrs.Tracing[string, string]("TestHandler"), // outermost: creates the span
		cqrs.Logging[string, string]("TestHandler"), // inside: logs WITH the span ctx
	)

	ctx := log.Into(context.Background(), logger)
	_, err := decorated(ctx, "cmd")
	require.NoError(t, err)
	_ = sync()

	spans := exp.GetSpans()
	require.NotEmpty(t, spans, "Tracing behavior must have exported a span")
	traceID := spans[0].SpanContext.TraceID().String()

	out := buf.String()
	assert.Contains(t, out, `"handler succeeded"`)
	assert.Contains(t, out, `"trace_id":"`+traceID+`"`,
		"log records inside the handler span must carry the span's trace_id")
	assert.Contains(t, out, `"span_id":"`)
}

// TestLogging_ErrorPathCarriesTraceID: the error log line is correlated too.
func TestLogging_ErrorPathCarriesTraceID(t *testing.T) {
	exp := withTestTracer(t)

	var buf bytes.Buffer
	logger, sync := log.New(log.Config{Level: "debug", Format: "json"}, &buf)
	t.Cleanup(func() { _ = sync() })

	boom := errors.New("boom")
	handler := func(_ context.Context, _ string) (string, error) { return "", boom }
	decorated := cqrs.Decorate(
		handler,
		cqrs.Tracing[string, string]("FailingHandler"),
		cqrs.Logging[string, string]("FailingHandler"),
	)

	ctx := log.Into(context.Background(), logger)
	_, err := decorated(ctx, "cmd")
	require.Error(t, err)
	_ = sync()

	spans := exp.GetSpans()
	require.NotEmpty(t, spans)
	traceID := spans[0].SpanContext.TraceID().String()

	out := buf.String()
	assert.Contains(t, out, `"handler failed"`)
	assert.Contains(t, out, `"trace_id":"`+traceID+`"`)
}
