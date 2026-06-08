package cqrs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// Tracing returns a Behavior that wraps the handler in an OpenTelemetry span.
// The span is named `name`. On error, RecordError and SetStatus(Error) are
// called. The span context is passed down to next so child spans are linked.
func Tracing[C, R any](name string) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			ctx, span := otel.Tracer("cqrs").Start(ctx, name)
			defer span.End()

			res, err := next(ctx, cmd)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return res, err
		}
	}
}
