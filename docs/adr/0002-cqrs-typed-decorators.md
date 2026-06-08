# ADR 0002 — CQRS: typed generic decorators over go-mediatr

**Status:** Accepted  
**Date:** 2026-06-08

## Context

The boilerplate needs a CQRS pipeline with cross-cutting behaviors (logging, tracing, metrics, validation, transactions, caching, audit). The primary candidate from the Go ecosystem is `go-mediatr`, a port of MediatR. It uses `interface{}` / `any` dispatch, reflection for handler registration, and a global registry — all of which make the dispatch path opaque, hard to test in isolation, and slow under the microscope.

## Decision

Implement typed generic decorators in `platform/cqrs`:

```go
type HandlerFunc[C, R any] func(context.Context, C) (R, error)
type Behavior[C, R any]    func(next HandlerFunc[C, R]) HandlerFunc[C, R]
func Decorate[C, R any](h HandlerFunc[C, R], behaviors ...Behavior[C, R]) HandlerFunc[C, R]
```

Each behavior is a compile-time-typed function. No reflection. No global registry. The decorated handler is just a function value that can be called, benchmarked, or replaced in tests.

## Consequences

- Full compile-time type safety: wrong command/result types are caught at compile time, not at runtime.
- The call graph is explicit and readable — `DecorateCreateOrderHandler` in `app/create_order.go` lists every behavior in order.
- Adding a new behavior requires changing one `Decorate` call; no configuration files or registration.
- The mediator indirection pattern (dispatch by type) is not supported; callers hold a typed handler reference. This is intentional — explicit beats implicit in a boilerplate designed to be read and modified.
- go-mediatr is excluded as a dependency.
