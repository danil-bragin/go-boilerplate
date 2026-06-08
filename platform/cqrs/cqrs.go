// Package cqrs provides a typed, compile-time-safe handler + pipeline-behavior
// pattern (an idiomatic alternative to a reflection-based mediator). A handler
// is HandlerFunc[C,R]; behaviors are decorators applied with Decorate, applied
// outermost-first (the first behavior in the list runs first / wraps the rest).
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
