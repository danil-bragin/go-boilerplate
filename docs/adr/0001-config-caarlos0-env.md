# ADR 0001 — Config: caarlos0/env over cleanenv / viper

**Status:** Accepted  
**Date:** 2026-06-08

## Context

The boilerplate needs a config loader that is idiomatic, AI-readable, and works well in containerised environments where all config arrives via environment variables. Candidates evaluated: `cleanenv`, `viper`, `caarlos0/env`.

`viper` supports many sources (files, etcd, consul) but that breadth introduces hidden precedence rules, reflection-heavy internals, and a global registry that makes test isolation hard. `cleanenv` is simpler but has a smaller ecosystem and less active maintenance. `caarlos0/env v11` uses struct tags (`env:"VAR" envDefault:"x"`) with generics-based `Parse[T]`, produces typed values at startup with fail-fast validation, and has no global state.

## Decision

Use `caarlos0/env v11` via `platform/config.Load[T]()`. Each service defines its own typed config struct; the loader is called once at startup and panics on missing required vars (via `config.Must[T]()`).

## Consequences

- Config structs are self-documenting: the struct tag is the variable name, the type is the Go type, and `envDefault` is the default.
- No config files in production — environment-variable-only is the 12-factor default for containers.
- Viper's dynamic reload and remote-source features are not available; if a service needs runtime config changes, OpenFeature feature flags are the intended mechanism.
- AI code generation works extremely well because the entire config contract is visible in one struct.
