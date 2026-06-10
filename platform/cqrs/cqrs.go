// Package cqrs provides a typed, compile-time-safe handler + pipeline-behavior
// pattern (an idiomatic alternative to a reflection-based mediator). A handler
// is HandlerFunc[C,R]; behaviors are decorators applied with Decorate, applied
// outermost-first (the first behavior in the list runs first / wraps the rest).
//
// # Required behavior order
//
// Tracing MUST be the OUTERMOST behavior:
//
//	cqrs.Decorate(handler,
//	    cqrs.Tracing[C, R]("Name"),    // outermost — opens the span
//	    cqrs.Logging[C, R]("Name"),    // inside — log lines get trace_id/span_id
//	    cqrs.Metrics[C, R]("Name"),
//	    cqrs.Validation[C, R](),
//	    // domain behaviors (Audit, Caching, …) innermost
//	)
//
// Tracing injects the span into ctx for everything it wraps; Logging emits
// context-aware records (XContext) that the platform/log traceHandler enriches
// with trace_id/span_id — but only when the span already exists in ctx, i.e.
// only when Logging runs INSIDE Tracing. With the order reversed, log↔trace
// correlation silently breaks (logs without trace_id), which is exactly the
// kind of failure nobody notices until an incident.
//
// Handlers never call other handlers ("cmd never calls cmd"): logic that two
// entry points need lives in a domain service the handlers adapt to, never in
// one handler dispatching another (which would re-run the whole pipeline and
// tangle transaction ownership).
package cqrs

import "context"

// HandlerFunc handles a command/query C and returns result R.
type HandlerFunc[C, R any] func(ctx context.Context, cmd C) (R, error)

// Behavior decorates a HandlerFunc with cross-cutting logic.
type Behavior[C, R any] func(next HandlerFunc[C, R]) HandlerFunc[C, R]

// Decorate wraps h with the given behaviors. The first behavior is outermost
// (runs first); the last is innermost (closest to h).
func Decorate[C, R any](h HandlerFunc[C, R], behaviors ...Behavior[C, R]) HandlerFunc[C, R] {
	for i := len(behaviors) - 1; i >= 0; i-- {
		h = behaviors[i](h)
	}
	return h
}
