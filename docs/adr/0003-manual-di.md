# ADR 0003 — Dependency Injection: manual wiring over fx / wire

**Status:** Accepted  
**Date:** 2026-06-08

## Context

The boilerplate needs a dependency injection strategy. Candidates: `uber-go/fx` (runtime DI container, reflection-based), `google/wire` (compile-time codegen), and manual constructor wiring. `fx` adds a runtime dependency graph with implicit invocation ordering, `Provide`/`Invoke` annotations, and a startup log that is hard to follow in unfamiliar codebases. `wire` generates boilerplate automatically but the generated code is non-obvious and requires learning the wire DSL. Both add friction for AI-assisted development and for developers reading the code for the first time.

## Decision

Use manual constructor wiring in each service's `NewApp` function. Teardown is handled by `platform/run.Closer`, which collects `func(ctx) error` closers and runs them in reverse registration order on shutdown.

```go
closer := run.NewCloser()
pool   := pg.NewPool(ctx, cfg.PG);  closer.Add(pool.Close)
kafka  := kafka.NewProducer(cfg.K); closer.Add(kafka.Close)
// ...
```

`samber/do v2` is documented as a future upgrade path if a service's dependency graph exceeds ~25–30 nodes.

## Consequences

- The entire wiring is visible in one function — no implicit ordering, no magic annotations.
- Compile-time safe: missing dependencies are compile errors, not startup panics.
- DI is boot-time only — zero steady-state runtime cost.
- Refactoring is straightforward: rename a constructor, fix the call sites, done.
- For very large services the `NewApp` function can grow long; the `samber/do v2` escape hatch is available when that becomes painful.
