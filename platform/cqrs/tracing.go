package cqrs

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// Tracing returns a Behavior that wraps the handler in an OpenTelemetry span.
// The span is named `name`. On error, RecordError and SetStatus(Error) are
// called. The span context is passed down to next so child spans are linked.
// If the handler panics, the span is marked as Error (with an exception event)
// and the panic is re-raised so it continues to propagate up the call stack.
func Tracing[C, R any](name string) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (res R, err error) {
			ctx, span := otel.Tracer("cqrs").Start(ctx, name)
			defer span.End()
			defer func() {
				if r := recover(); r != nil {
					perr := fmt.Errorf("panic: %v", r)
					span.RecordError(perr)
					span.SetStatus(codes.Error, perr.Error())
					panic(r) // re-raise after recording
				}
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
			}()
			res, err = next(ctx, cmd)
			return res, err
		}
	}
}
