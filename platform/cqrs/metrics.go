package cqrs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics returns a Behavior that records a call counter and duration histogram
// for each handler invocation via the global OTel meter.
//
// Instruments are created once per Behavior closure (keyed by `name`) using
// otel.Meter("cqrs"). The global meter deduplicates identical instrument names,
// so multiple Decorate calls with the same name are safe. If instrument
// creation fails (degrade gracefully), the behavior still calls next.
func Metrics[C, R any](name string) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		m := otel.Meter("cqrs")

		counter, counterErr := m.Int64Counter("cqrs.handler.calls",
			metric.WithDescription("Number of handler invocations"),
		)
		histogram, histErr := m.Float64Histogram("cqrs.handler.duration_ms",
			metric.WithDescription("Handler duration in milliseconds"),
			metric.WithUnit("ms"),
		)

		return func(ctx context.Context, cmd C) (R, error) {
			start := time.Now()
			res, err := next(ctx, cmd)
			dur := float64(time.Since(start).Milliseconds())

			status := "ok"
			if err != nil {
				status = "error"
			}
			attrs := metric.WithAttributes(
				attribute.String("handler", name),
				attribute.String("status", status),
			)

			if counterErr == nil {
				counter.Add(ctx, 1, attrs)
			}
			if histErr == nil {
				histogram.Record(ctx, dur, attrs)
			}
			return res, err
		}
	}
}
