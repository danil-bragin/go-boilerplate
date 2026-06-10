# Conventions: file organisation, package boundaries, and tooling

Practical reference for contributors. Read this before adding a new file, package, or tool dependency.

---

## 1. File organisation

**One cohesive responsibility per file.** A file should be named for the concept it owns. If a file mixes two unrelated concerns or grows past roughly 200–250 lines, split it. Do not create trivially tiny files (a 10-line helper does not need its own file).

### Core rules

| Rule | Example |
|---|---|
| A type and its constructor live in the same file | `health.go` defines `Health` and `New()` |
| `Config` structs go in `config.go` | `platform/messaging/kafka/config.go`, `platform/servicekit/config.go` |
| HTTP handlers (request/response logic) are separate from routing wiring | `handlers.go` vs `routes.go` in `examples/gateway/` |
| Each sub-system or behaviour gets its own file | `relay.go`, `cleanup.go`, `consumers.go` in `platform/servicekit/` |

### Real examples from this repo

**`platform/observability/health`** — three files, each with a distinct responsibility:

| File | Owns |
|---|---|
| `health.go` | `Health` type, `New()`, `AddReadiness`, `SetNotLive/Ready` |
| `check.go` | `Check` function type and `CheckFunc` adapter (consumer-defined interface seam) |
| `handlers.go` | `LivezHandler()`, `ReadyzHandler()`, `Mount()` (HTTP wiring) |

**`platform/servicekit`** — the shared consumer harness split by sub-system (package `servicekit`):

| File | Owns |
|---|---|
| `service.go` | `Service` type, `New()` constructor (full wiring: logger, telemetry, pg, kafka, health, admin server) |
| `config.go` | `Config` struct (embeddable base config for all consumer services) |
| `consumers.go` | `EnsureTopics`, `AddConsumer` (Kafka consumer wiring with DLT) |
| `relay.go` | `AddOutboxRelay`, `DefaultOutboxPublisher` (outbox relay + cleaner wiring) |
| `cleanup.go` | `AddAuditCleanup`, `startInboxCleanup` (retention cleanup goroutines) |
| `lifecycle.go` | `Start`, `Stop` (goroutine launch and ordered shutdown) |

**`examples/gateway`** — the REST edge service split by concern:

| File | Owns |
|---|---|
| `gateway.go` | `App` type, `NewApp()`, `Option` / `WithVerifier`, `Start`, `Stop` (full wiring) |
| `config.go` | `Config` struct (gateway-specific env fields) |
| `deps.go` | optional dependency builders: `buildCache`, `buildBlob`, `buildFeatureFlags`, `buildVerifier` |
| `routes.go` | `applyEdgeSecurity`, `mountAPIRoutes`, `mountAttachmentRoutes` (HTTP routing) |

### Guideline

Split when a file mixes two or more responsibilities **or** exceeds ~200–250 lines. The split point is usually obvious: pull the sub-system into its own file named for that sub-system. Do not split a cohesive 150-line file just to hit an arbitrary line target.

---

## 2. Package boundaries

| Zone | Rule |
|---|---|
| `platform/` | Reusable starter kit — zero business logic, never imports `examples/` |
| `examples/` | Deletable demonstrations — show how to wire `platform/`; safe to delete entirely |
| Per package | One cohesive concern per package. Packages are named for what they provide, not what uses them |
| Interfaces | Defined by the **consumer**, not the producer (`health.Check`, `outbox.Publisher`). Keeps `platform/` packages independently usable |

### Platform group map

`platform/` is organised into domain groups (≤2 levels). Use the grouped import paths:

| Group | Packages |
|---|---|
| `platform/messaging/` | `kafka`, `retry`, `serde`, `outbox`, `inbox`, `outboxkafka` |
| `platform/observability/` | `log`, `telemetry`, `health` |
| `platform/web/` | `httpserver`, `httpx`, `ratelimit` |
| `platform/security/` | `auth`, `authz`, `audit` |
| `platform/storage/` | `pg`, `cache`, `blob` |
| standalone | `config`, `run`, `cqrs`, `resilience`, `featureflags`, `testkit` |

`platform/` packages never depend on each other circularly; the `messaging/outboxkafka` package exists specifically to bridge `messaging/outbox` and `messaging/kafka` without creating a cycle.

### Standard example-service internal layout

```
examples/<service>/
├── cmd/<service>/main.go
├── internal/
│   ├── app/           command/query handlers + Decorate wiring
│   ├── transport/     Kafka consumers (inbox.ProcessOnce wrapping)
│   ├── store/         sqlc queries + pgx; never opens transactions
│   └── migrations/    goose embed SQL
└── <service>.go       NewApp / Start / Stop / Closer
```

Role-specific extras: `gateway` adds `internal/api/` (oapi-codegen), `internal/attachments/`, `internal/projection/`; `notifications` is terminal (transport + inbox only, no app layer).

Deleting `examples/`, `proto/`, and `gen/` leaves a clean `platform/`-only starter — this works because the boundary is enforced.

---

## 3. Tooling map

| Tool | Purpose | Key command(s) |
|---|---|---|
| **`just`** | Task runner — replaces Makefile / Taskfile | `just` (list all), see §4 for daily recipes |
| **`lefthook`** | Git hooks manager | `just hooks` to install; runs `fmt`+`lint`+`build` on pre-commit, `test -short` on pre-push |
| **`gofumpt`** | Strict `gofmt` superset (stricter blank-line rules) | Run via `golangci-lint fmt` or the pre-commit hook (`go run mvdan.cc/gofumpt@latest`) |
| **`goimports`** | Adds/removes imports, enforces local-prefix grouping | Run via `golangci-lint fmt` or the pre-commit hook; local prefix is `go-boilerplate` |
| **`golangci-lint`** | Aggregates all linters in one pass | `just lint` / `just lint-fix` |
| **`air`** | Hot-reload for local development | `just dev <svc>` (optional install: `go install github.com/air-verse/air@latest`) |
| **`govulncheck`** | Vulnerability scan against Go vuln database | `just vuln` |
| **Dependabot** | Automated dependency-update PRs | Configured in `.github/dependabot.yml`; no local action required |
| **goreleaser** | Release tooling — builds binaries + Docker images | `just release` (local snapshot, no publish) |

### Enabled linters (`.golangci.yml`)

`errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `revive`, `bodyclose`, `errorlint`, `gosec`, `gocritic` (diagnostic + style + performance tags), `misspell`, `unconvert`, `prealloc`, `perfsprint`, `errname`, `nilerr`, `nilnil`, `wastedassign`, `usestdlibvars`, `copyloopvar`

### Enabled formatters (`.golangci.yml`)

`gofumpt` (module-path: `go-boilerplate`) + `goimports` (local-prefixes: `go-boilerplate`)

---

## 4. Dev environment setup

### Prerequisites

| Tool | Install | Required? |
|---|---|---|
| Go 1.25+ | https://go.dev/dl/ | Required |
| Docker | https://docs.docker.com/get-docker/ | Required (integration tests + local stack) |
| `just` | `brew install just` or `cargo install just` | Required |
| `lefthook` | `brew install lefthook` | Required (git hooks) |
| `golangci-lint` | `brew install golangci-lint` | Required (pre-commit lint) |
| `air` | `go install github.com/air-verse/air@latest` | Optional (hot-reload) |

`gofumpt` and `goimports` are **not** installed separately — the pre-commit hook runs them via `go run` so no local install is needed.

### First-time setup

```bash
git clone <repo-url>
cd go-boilerplate
just hooks        # install lefthook git hooks (one-time)
```

### Daily recipes

```bash
just dev gateway        # hot-reload gateway (air)
just dev orders         # hot-reload orders service

just test-unit          # fast lane: unit + functional tests only (no Docker)
just test-integration   # full lane: all tests including integration (requires Docker)
just test-cover         # unit tests + coverage summary

just lint               # run golangci-lint
just fmt                # format via golangci-lint fmt (gofumpt + goimports)
just audit              # fmt + lint + vuln + unit tests (pre-merge gate)

just up                 # start core infra (postgres, redpanda, redis, minio, keycloak)
just up-obs             # core + observability stack
just up-apps            # core + four app services (built from source)
just up-full            # everything
just down               # stop everything + remove volumes

just gen-mocks          # regenerate moq mocks after platform interface changes
just token              # fetch a Keycloak demo token for manual API calls
just hooks              # reinstall lefthook git hooks
```

Run `just` with no arguments to list every available recipe.

---

## 5. Error handling and naming conventions

**Error wrapping.** Use `fmt.Errorf` with `%w` and a `pkg: verb: ` prefix so the call stack is readable without a stack trace:

```go
return fmt.Errorf("orders: create order: %w", err)
```

**Sentinel errors.** Name with the `Err` prefix and export from the package that owns the concept:

```go
var ErrNotFound = errors.New("not found")
```

**Godoc.** Every exported symbol must have a doc comment beginning with the symbol name (enforced by the `revive` linter):

```go
// Health aggregates liveness and readiness state.
type Health struct { ... }
```

**Context.** `context.Context` is always the first parameter of any function that accepts one. Never store a context in a struct field except where the Go stdlib pattern requires it (noted explicitly in the code with `//nolint:containedctx` and a comment explaining why).

**Naming.** Constructors are `New` or `New<Type>`. Config structs are `Config`. Unexported type aliases used only within a file are fine; exported types used across packages go in the package-level file named for the concept.

---

## 6. Cache key convention

Every cache key is **versioned**: `<svc>:v<N>:<entity>:<id>`.

| Segment | Meaning | Example |
|---|---|---|
| `<svc>` | Short service prefix (also namespaces services sharing one Redis) | `gw` |
| `v<N>` | Result-shape version — **bump N whenever the cached value's shape changes** | `v1` |
| `<entity>` | What is cached | `order` |
| `<id>` | Entity identifier | `1b4e28ba-…` |

Example: `gw:v1:order:1b4e28ba-2fa1-11d2-883f-0016d3cca427` (see `examples/gateway/internal/app.OrderCacheKey`).

Rules:

1. **One helper per key shape.** Define a single `XxxCacheKey(id)` function and use it from BOTH the read path (`cqrs.Caching` keyFor) and every write path that busts the entry (`cache.Delete`). Two hand-rolled format strings will eventually drift and invalidation silently misses.
2. **Bump the version on shape change.** When the cached struct gains/renames fields or changes semantics, bump `v1` → `v2`. Old entries become unreachable and expire naturally — no flush, no mixed-shape unmarshalling.
3. **Never reuse a version.** A rolled-back deploy that reused `v2` with a different shape would poison the cache for the roll-forward.
