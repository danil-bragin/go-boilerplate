// Package telemetry configures the OpenTelemetry tracer and meter providers
// together with W3C context propagation. When Enabled is false, a true no-op
// tracer provider is installed so span creation is allocation-free.
//
// Metrics pipeline:
//   - When MetricsPrometheus is true (the default) a Prometheus-pull reader is
//     always registered, even when Enabled is false. This ensures cqrs.Metrics
//     instruments are live for local development without a collector.
//   - When Enabled is true an OTLP-push reader is also registered so metrics
//     flow to the collector alongside traces.
//
// Use SetupWithMetrics to obtain the http.Handler for the /metrics endpoint.
// The top-level Setup function is kept for backward compatibility and discards
// the metrics handler (useful when the caller mounts /metrics separately or
// does not need Prometheus exposition).
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	noop "go.opentelemetry.io/otel/trace/noop"
)

// Config controls telemetry setup.
type Config struct {
	ServiceName       string `env:"OTEL_SERVICE_NAME"         envDefault:"service"`
	OTLPEndpoint      string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	Enabled           bool   `env:"OTEL_ENABLED"              envDefault:"false"`
	MetricsPrometheus bool   `env:"OTEL_METRICS_PROMETHEUS"   envDefault:"true"`
}

// ShutdownFunc flushes and stops telemetry providers.
type ShutdownFunc func(ctx context.Context) error

// Setup installs the global tracer + meter providers and W3C propagation.
// It keeps the original signature for backward compatibility.
//
// When MetricsPrometheus is true (the default) a real MeterProvider is always
// installed (even when Enabled=false) so that instruments created via
// otel.Meter() are live. The /metrics Prometheus handler can be obtained by
// calling SetupWithMetrics instead.
func Setup(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	shutdown, _, err := SetupWithMetrics(ctx, cfg)
	return shutdown, err
}

// SetupWithMetrics is the full-featured variant of Setup. It returns:
//   - ShutdownFunc – flushes and stops all providers.
//   - http.Handler – a Prometheus /metrics handler (nil when MetricsPrometheus
//     is false).
//   - error
func SetupWithMetrics(ctx context.Context, cfg Config) (ShutdownFunc, http.Handler, error) {
	// Always configure W3C propagation regardless of Enabled.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	var shutdowns []func(context.Context) error
	var metricsHandler http.Handler

	// -----------------------------------------------------------------------
	// Tracer provider
	// -----------------------------------------------------------------------
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
	} else {
		res, err := resource.New(
			ctx,
			resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
		)
		if err != nil {
			if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
				slog.Warn("telemetry: partial resource", "error", err)
			} else {
				return nil, nil, fmt.Errorf("telemetry: resource: %w", err)
			}
		}

		traceExp, err := otlptracegrpc.New(
			ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("telemetry: otlp trace exporter: %w", err)
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(traceExp),
		)
		otel.SetTracerProvider(tp)

		shutdowns = append(shutdowns, func(_ context.Context) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return tp.Shutdown(ctx)
		})
	}

	// -----------------------------------------------------------------------
	// Meter provider
	// -----------------------------------------------------------------------
	var mpReaders []sdkmetric.Option

	if cfg.MetricsPrometheus {
		// Use an explicit registry so we don't pollute the global default one
		// (important for test isolation).
		reg := promclient.NewRegistry()
		promExp, err := promexporter.New(
			promexporter.WithRegisterer(reg),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
		}
		mpReaders = append(mpReaders, sdkmetric.WithReader(promExp))
		metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	}

	if cfg.Enabled {
		metricExp, err := otlpmetricgrpc.New(
			ctx,
			otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("telemetry: otlp metric exporter: %w", err)
		}
		mpReaders = append(mpReaders, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)))
	}

	if len(mpReaders) > 0 {
		mp := sdkmetric.NewMeterProvider(mpReaders...)
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, func(_ context.Context) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mp.Shutdown(ctx); err != nil {
				// Metric flush errors (e.g. collector unreachable) are best-effort;
				// log them but do not fail shutdown so the process can exit cleanly.
				slog.Warn("telemetry: meter provider shutdown error", "error", err)
			}
			return nil
		})
	}

	shutdown := func(_ context.Context) error {
		var errs []error
		for i := len(shutdowns) - 1; i >= 0; i-- {
			if err := shutdowns[i](context.Background()); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	return shutdown, metricsHandler, nil
}
