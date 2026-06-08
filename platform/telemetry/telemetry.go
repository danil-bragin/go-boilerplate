// Package telemetry configures the OpenTelemetry tracer provider and W3C
// context propagation. When Enabled is false, a true no-op provider is
// installed so span creation is allocation-free. A meter/metrics provider is
// added in a later sub-project.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	noop "go.opentelemetry.io/otel/trace/noop"
)

// Config controls telemetry setup.
type Config struct {
	ServiceName  string `env:"OTEL_SERVICE_NAME" env-default:"service"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" env-default:"localhost:4317"`
	Enabled      bool   `env:"OTEL_ENABLED" env-default:"false"`
}

// ShutdownFunc flushes and stops telemetry providers.
type ShutdownFunc func(ctx context.Context) error

// Setup installs the global tracer provider and W3C propagation.
// It returns a shutdown function that flushes the exporter.
//
// When Enabled is false a true no-op provider is installed — span creation
// costs zero allocations. When Enabled is true the full SDK pipeline is
// wired: resource → OTLP/gRPC batcher → sdktrace.TracerProvider.
func Setup(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	// FIX 3 — disabled path: install a true no-op provider and return
	// immediately, without allocating any SDK objects.
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		// Still set propagation so context propagation works across services.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	// FIX 1 — resource.New can return ErrPartialResource or ErrSchemaURLConflict
	// alongside a valid partial resource; treat these as warnings, not fatal.
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
			slog.Warn("telemetry: partial resource", "error", err)
		} else {
			return nil, fmt.Errorf("telemetry: resource: %w", err)
		}
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp),
	)
	otel.SetTracerProvider(tp)

	// FIX 2 — ignore the incoming (possibly cancelled) ctx so that flush
	// succeeds even when the caller's root context is cancelled on SIGTERM.
	return func(_ context.Context) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return tp.Shutdown(ctx)
	}, nil
}
