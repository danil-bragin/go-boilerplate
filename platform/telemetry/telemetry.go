// Package telemetry configures OpenTelemetry tracer and meter providers.
// When Enabled is false, providers are installed but no exporter is wired,
// so spans/metrics are cheap no-ops — convenient for tests and local runs.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config controls telemetry setup.
type Config struct {
	ServiceName  string `env:"OTEL_SERVICE_NAME" env-default:"service"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" env-default:"localhost:4317"`
	Enabled      bool   `env:"OTEL_ENABLED" env-default:"false"`
}

// ShutdownFunc flushes and stops telemetry providers.
type ShutdownFunc func(ctx context.Context) error

// Setup installs global tracer/meter providers and W3C propagation.
// It returns a shutdown function that flushes exporters.
func Setup(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))

	if cfg.Enabled {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(ctx)
	}, nil
}
