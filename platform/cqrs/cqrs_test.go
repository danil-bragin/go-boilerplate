package cqrs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/log"
)

// TestDecorate_OrderAndComposition verifies that behaviors are applied
// outermost-first (first in the list wraps the rest) and that the handler
// runs in the middle with results propagated correctly.
func TestDecorate_OrderAndComposition(t *testing.T) {
	var trace []string

	makeBehavior := func(tag string) cqrs.Behavior[string, string] {
		return func(next cqrs.HandlerFunc[string, string]) cqrs.HandlerFunc[string, string] {
			return func(ctx context.Context, cmd string) (string, error) {
				trace = append(trace, tag+":before")
				res, err := next(ctx, cmd)
				trace = append(trace, tag+":after")
				return res, err
			}
		}
	}

	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, cmd string) (string, error) {
		trace = append(trace, "handler")
		return "result:" + cmd, nil
	})

	decorated := cqrs.Decorate(handler, makeBehavior("A"), makeBehavior("B"))
	res, err := decorated(context.Background(), "input")

	require.NoError(t, err)
	require.Equal(t, "result:input", res)
	require.Equal(t, []string{"A:before", "B:before", "handler", "B:after", "A:after"}, trace)
}

// TestDecorate_NoBehaviors returns the handler unchanged.
func TestDecorate_NoBehaviors(t *testing.T) {
	handler := cqrs.HandlerFunc[int, int](func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	})
	decorated := cqrs.Decorate(handler)
	res, err := decorated(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, 10, res)
}

// TestDecorate_ErrorPropagates verifies that errors from the handler are
// passed through the behavior chain unchanged.
func TestDecorate_ErrorPropagates(t *testing.T) {
	sentinel := errors.New("sentinel")
	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})
	var afterCalled bool
	behavior := cqrs.Behavior[string, string](func(next cqrs.HandlerFunc[string, string]) cqrs.HandlerFunc[string, string] {
		return func(ctx context.Context, cmd string) (string, error) {
			res, err := next(ctx, cmd)
			afterCalled = true
			return res, err
		}
	})
	decorated := cqrs.Decorate(handler, behavior)
	_, err := decorated(context.Background(), "x")
	require.ErrorIs(t, err, sentinel)
	require.True(t, afterCalled)
}

// --- Logging tests ---

func TestLogging_LogsSuccess(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "debug", Format: "json"}, &buf)
	ctx := log.Into(context.Background(), logger)

	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Logging[string, string]("TestHandler"))
	res, err := decorated(ctx, "input")

	require.NoError(t, err)
	require.Equal(t, "ok", res)

	lines := splitJSONLines(buf.Bytes())
	require.Len(t, lines, 2, "expected start (debug) and end (info) log lines")

	// first line: debug start
	require.Equal(t, "debug", lines[0]["level"])
	require.Equal(t, "TestHandler", lines[0]["handler"])

	// second line: info success
	require.Equal(t, "info", lines[1]["level"])
	require.Equal(t, "TestHandler", lines[1]["handler"])
	require.Equal(t, "ok", lines[1]["status"])
	_, hasDur := lines[1]["duration_ms"]
	require.True(t, hasDur, "expected duration_ms field")
}

func TestLogging_LogsError(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := log.New(log.Config{Level: "debug", Format: "json"}, &buf)
	ctx := log.Into(context.Background(), logger)

	sentinel := errors.New("boom")
	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})
	decorated := cqrs.Decorate(handler, cqrs.Logging[string, string]("FailHandler"))
	_, err := decorated(ctx, "x")

	require.ErrorIs(t, err, sentinel)

	lines := splitJSONLines(buf.Bytes())
	require.Len(t, lines, 2, "expected start (debug) and end (error) log lines")

	// second line: error
	require.Equal(t, "error", lines[1]["level"])
	require.Equal(t, "FailHandler", lines[1]["handler"])
	require.Equal(t, "error", lines[1]["status"])
	_, hasDur := lines[1]["duration_ms"]
	require.True(t, hasDur, "expected duration_ms field")
}

// splitJSONLines parses newline-delimited JSON into a slice of maps.
func splitJSONLines(b []byte) []map[string]any {
	var result []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err == nil {
			result = append(result, m)
		}
	}
	return result
}

// --- Tracing tests ---

func TestTracing_RecordsSpanName(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, cmd string) (string, error) {
		return cmd, nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Tracing[string, string]("MySpan"))
	_, err := decorated(context.Background(), "hello")
	require.NoError(t, err)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "MySpan", spans[0].Name)
	require.Equal(t, codes.Unset, spans[0].Status.Code)
}

func TestTracing_RecordsErrorOnSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	sentinel := errors.New("trace-error")
	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})
	decorated := cqrs.Decorate(handler, cqrs.Tracing[string, string]("ErrSpan"))
	_, err := decorated(context.Background(), "x")
	require.ErrorIs(t, err, sentinel)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "ErrSpan", spans[0].Name)
	require.Equal(t, codes.Error, spans[0].Status.Code)

	// RecordError produces an "exception" event
	var hasException bool
	for _, ev := range spans[0].Events {
		if ev.Name == "exception" {
			hasException = true
		}
	}
	require.True(t, hasException, "expected exception event from RecordError")
}

// --- Metrics tests ---

func TestMetrics_RecordsWithoutPanic(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	sentinel := errors.New("metric-error")

	successHandler := cqrs.HandlerFunc[string, string](func(_ context.Context, cmd string) (string, error) {
		return cmd, nil
	})
	errorHandler := cqrs.HandlerFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})

	decoratedOK := cqrs.Decorate(successHandler, cqrs.Metrics[string, string]("MetricHandler"))
	decoratedErr := cqrs.Decorate(errorHandler, cqrs.Metrics[string, string]("MetricHandler"))

	res, err := decoratedOK(context.Background(), "hello")
	require.NoError(t, err)
	require.Equal(t, "hello", res)

	_, err = decoratedErr(context.Background(), "x")
	require.ErrorIs(t, err, sentinel)

	// Collect and verify at least one metric with the right name exists.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var foundCount, foundHistogram bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "cqrs.handler.calls" {
				foundCount = true
			}
			if m.Name == "cqrs.handler.duration_ms" {
				foundHistogram = true
			}
		}
	}
	require.True(t, foundCount, "expected cqrs.handler.calls metric")
	require.True(t, foundHistogram, "expected cqrs.handler.duration_ms metric")
}

// --- Validation tests ---

type createOrderCmd struct {
	CustomerID string `validate:"required"`
	Amount     int
}

type badValidatableCmd struct{}

func (b badValidatableCmd) Validate() error {
	return errors.New("custom-validate-error")
}

type goodValidatableCmd struct{}

func (g goodValidatableCmd) Validate() error { return nil }

func TestValidation_RejectsInvalidStruct(t *testing.T) {
	var nextCalled bool
	handler := cqrs.HandlerFunc[createOrderCmd, string](func(_ context.Context, _ createOrderCmd) (string, error) {
		nextCalled = true
		return "ok", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Validation[createOrderCmd, string]())

	// Empty CustomerID → required validation fails
	_, err := decorated(context.Background(), createOrderCmd{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cqrs: validation:")
	require.False(t, nextCalled, "next must not be called on validation failure")
}

func TestValidation_PassesValidStruct(t *testing.T) {
	var nextCalled bool
	handler := cqrs.HandlerFunc[createOrderCmd, string](func(_ context.Context, _ createOrderCmd) (string, error) {
		nextCalled = true
		return "done", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Validation[createOrderCmd, string]())

	res, err := decorated(context.Background(), createOrderCmd{CustomerID: "cust-1", Amount: 100})
	require.NoError(t, err)
	require.Equal(t, "done", res)
	require.True(t, nextCalled)
}

func TestValidation_UsesValidatableInterface_Error(t *testing.T) {
	var nextCalled bool
	handler := cqrs.HandlerFunc[badValidatableCmd, string](func(_ context.Context, _ badValidatableCmd) (string, error) {
		nextCalled = true
		return "ok", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Validation[badValidatableCmd, string]())

	_, err := decorated(context.Background(), badValidatableCmd{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cqrs: validation:")
	require.Contains(t, err.Error(), "custom-validate-error")
	require.False(t, nextCalled)
}

func TestValidation_UsesValidatableInterface_OK(t *testing.T) {
	handler := cqrs.HandlerFunc[goodValidatableCmd, string](func(_ context.Context, _ goodValidatableCmd) (string, error) {
		return "fine", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Validation[goodValidatableCmd, string]())

	res, err := decorated(context.Background(), goodValidatableCmd{})
	require.NoError(t, err)
	require.Equal(t, "fine", res)
}

func TestValidation_SkipsNonStruct(t *testing.T) {
	// For a non-struct C (e.g. string), validation is skipped — no error, next called.
	handler := cqrs.HandlerFunc[string, string](func(_ context.Context, cmd string) (string, error) {
		return cmd + "-result", nil
	})
	decorated := cqrs.Decorate(handler, cqrs.Validation[string, string]())

	res, err := decorated(context.Background(), "raw")
	require.NoError(t, err)
	require.Equal(t, "raw-result", res)
}
