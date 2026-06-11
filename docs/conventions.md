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
| `consumers.go` | `EnsureTopics`, `AddConsumer`, `AddConsumerWithRetry` (Kafka consumer wiring with DLT / tiered retry) |
| `relay.go` | `AddOutboxRelay`, `DefaultOutboxPublisher`, `RegisterSchema` (outbox relay + serde wiring) |
| `cleanup.go` | `AddAuditCleanup`, `startInboxCleanup` (retention cleanup goroutines) |
| `lifecycle.go` | `Start`, `Stop` (goroutine launch and readiness-first ordered shutdown) |
| `options.go` | `WithoutKafka` / `WithoutPG` construction options |
| `main.go` | `Main` shared process entry point (automaxprocs, signal handling, Closer teardown, exit codes) |

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
| `platform/messaging/` | `kafka`, `retry`, `consume`, `msgctx`, `serde`, `outbox`, `inbox`, `outboxkafka` |
| `platform/observability/` | `log`, `telemetry`, `health` |
| `platform/web/` | `httpserver`, `httpx`, `ratelimit` |
| `platform/security/` | `auth`, `authz`, `audit` |
| `platform/storage/` | `pg`, `cache`, `blob` |
| `platform/testkit/` | `fakes`, `mockhttp`, `mocks`, `fixtures`, `goleakopts`, `traffic` |
| standalone | `config`, `run`, `cqrs`, `resilience`, `featureflags`, `servicekit`, `apperr`, `i18n`, `clock` |

`platform/` packages never depend on each other circularly; the `messaging/outboxkafka` package exists specifically to bridge `messaging/outbox` and `messaging/kafka` without creating a cycle.

### Standard example-service internal layout

```
examples/<service>/
├── cmd/<service>/main.go
├── internal/
│   ├── domain/<agg>/  THE business layer: error codes (codes.go), Repository
│   │                  interface (consumer-side) + pg implementation, Service
│   │                  owning every business rule (see §9 Layering)
│   ├── app/           command/query handlers — thin adapters over the domain
│   │                  Service + Decorate pipeline wiring
│   ├── transport/     Kafka consumers — decode + dispatch to the Service
│   ├── store/         sqlc queries + pgx; never opens transactions
│   └── migrations/    goose embed SQL
└── <service>.go       NewApp / Start / Stop / Closer
```

Role-specific extras: `gateway` adds `internal/api/` (oapi-codegen), `internal/apperrs/` (its `GATEWAY_*` codes + i18n catalogs), `internal/attachments/`, `internal/projection/`; `notifications` is terminal — transport + a minimal `internal/domain/notification.Service`, no app or store layer.

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
| Go 1.26+ | https://go.dev/dl/ | Required |
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

just up                 # start core infra (postgres, redpanda, redis, seaweedfs, keycloak)
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

**Coded application errors.** Errors that surface to API clients or drive retry/DLT semantics are not plain sentinels — they carry a registered `platform/apperr` code. See §10.

**Godoc.** Every exported symbol must have a doc comment beginning with the symbol name (enforced by the `revive` linter):

```go
// Health aggregates liveness and readiness state.
type Health struct { ... }
```

**Context.** `context.Context` is always the first parameter of any function that accepts one. Never store a context in a struct field except where the Go stdlib pattern requires it (noted explicitly in the code with `//nolint:containedctx` and a comment explaining why).

**Naming.** Constructors are `New` or `New<Type>`. Config structs are `Config`. Unexported type aliases used only within a file are fine; exported types used across packages go in the package-level file named for the concept.

**Secrets in config.** Credential-bearing config fields (passwords, API keys, DSNs with embedded passwords) use `config.Secret` instead of `string`: it prints `[REDACTED]` for `%v`/`%+v`/`%#v` and in slog output, so a dumped config struct never leaks credentials. The raw value is accessed explicitly via `Reveal()` at the single call site that hands it to a client library — `git grep "\.Reveal()"` lists every place a secret leaves the config layer. Examples: `pg.Config.DSN`, `blob.Config.SecretKey`.

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

---

## 7. CQRS pipeline: `StandardPipeline` vs raw `Decorate`

**`cqrs.StandardPipeline` is the default.** It assembles the canonical stack
(Tracing → Logging → Metrics → Validation) in the required order, with fluent
options for the conditional behaviors:

```go
return cqrs.StandardPipeline[CreateOrder, CreateOrderResult]("CreateOrder").
    Use(audit.Audit[CreateOrder, CreateOrderResult](auditStore, "order:create",
        func(cmd CreateOrder) string { return cmd.OrderID })).
    Decorate(handler)
```

Use `WithCache` for queries, `WithAuthz`/`WithDeadline` as needed, and `Use(...)`
for domain behaviors (Audit, custom checks) — they run innermost. `WithTransaction`
is for command handlers invoked OUTSIDE a consumer; handlers run via
`inbox.ProcessOnce` already execute inside the inbox transaction (see the
`cqrs.Pipeline.WithTransaction` godoc).

**Raw `cqrs.Decorate` is the escape hatch** for genuinely custom behavior orders
(e.g. a behavior that must wrap Tracing, or a stack that intentionally drops a
standard behavior). If you reach for it, comment WHY the canonical order doesn't
fit — otherwise reviewers should push back to `StandardPipeline`.

Resilience (retries, circuit breaking, rate limiting) is never a pipeline
behavior: it stays at the transport level (httpserver middleware, kafka/retry
escalation, `platform/resilience` around outbound calls).

---

## 8. Topic env naming

**Rule: a topic env is named after the TOPIC it points at, never after a
service.** `orders.commands` → `ORDERS_COMMANDS_TOPIC`, `orders.events` →
`ORDERS_EVENTS_TOPIC`, `payments.events` → `PAYMENTS_EVENTS_TOPIC` — in
every service that touches the topic, producer or consumer. No
`GATEWAY_*_TOPIC`-style prefixes.

Why: each container has its own env namespace, so several services reading
the same env name is not a conflict — it is the point. "Same topic = same
env name everywhere" means one compose/.env line per topic per service, no
mental mapping table, and a single-process test (e2e) sets each name once.

---

## 9. Layering: domain service + repository, uniformly

**Decision: every example service has the same `internal/domain/<aggregate>` layer — service + repository — no matter how little logic it holds today.** Notifications' service is a few lines; that is correct, not a smell. The examples are *templates*: people copy them to start real services, and a copied template with a ready seam beats one where the first business rule lands in a Kafka handler because there was nowhere else to put it. Uniformity beats YAGNI here.

### What lives where

| Piece | Location | Rule |
|---|---|---|
| Error codes | `internal/domain/<agg>/codes.go` | const block + `apperr.Register` in `init()` (see §10) |
| Repository interface | `internal/domain/<agg>` (next to the Service) | defined **consumer-side**: the Service declares what it needs; storage adapters satisfy it (`pg.go`) |
| Business rules | `internal/domain/<agg>/service.go` | state machines, decision rules, outbox event enqueueing — ALL of it |
| cqrs handlers | `internal/app/` | thin adapters: validate-tag the command, decorate with the pipeline, delegate to the Service |
| Kafka consumers | `internal/transport/` | thin adapters: decode + dispatch to the Service, nothing else |
| Background loops | `internal/app/` (e.g. the orders unpaid watcher) | own the loop + transaction boundary (`pg.RunInTx`), delegate decisions to the Service |

Reference implementations: `examples/orders/internal/domain/order` (full: state machine, codes, repository, service), `examples/payments/internal/domain/payment` (decision rule + injected clock), `examples/notifications/internal/domain/notification` (deliberately minimal).

### cmd never calls cmd

A command handler never invokes another command handler. Logic that two entry points both need lives in the domain Service — that is exactly why the Service exists. Handler-calls-handler creates hidden pipelines-inside-pipelines (double validation, double tx semantics, double audit) and untestable coupling. The orders payment-outcome logic moved from the transport consumer into `order.Service.ApplyPaymentOutcome` for precisely this reason.

### Ambient transaction, formalized

Repositories resolve their query surface from the **context**, never from a stored connection and never by opening transactions:

```go nocompile
func (r *PgRepository) q(ctx context.Context) *gen.Queries {
    return gen.New(pg.FromContext(ctx, r.pool))
}
```

`pg.FromContext` returns the transaction bound to ctx, or the writer pool when none is active. The same repository therefore works unchanged under all three transaction owners:

1. **`inbox.ProcessOnce`** — the production consumer path: domain write + outbox enqueue + inbox dedup marker commit atomically.
2. **`cqrs.Transaction` / `Pipeline.WithTransaction`** — command handlers invoked outside a consumer.
3. **explicit `pg.RunInTx`** — background loops (the unpaid watcher wraps each order in its own transaction).

**Writer-fallback hazard:** with no transaction in ctx the fallback is the writer pool with per-statement auto-commit — atomicity silently disappears. Every command path must run under one of the three owners above; this is enforced by convention and by integration tests asserting outbox atomicity (see the `pg.FromContext` godoc).

**Goroutine gotcha: a new goroutine is a new transaction boundary.** Never pass a ctx carrying a transaction into a spawned goroutine: `pgx.Tx` is bound to a single connection and is not safe for concurrent use, and the parent may commit or roll back while the goroutine still holds the tx. If concurrent work needs the database, each goroutine opens its own `pg.RunInTx` with a fresh (non-transactional) context — and accepts that it commits independently of the parent.

---

## 10. Error model

The application error model is `platform/apperr`: a **flat UPPER_SNAKE code** with an HTTP status, a permanence flag, structured params, and a developer message template. Codes owned by a service carry its prefix (`ORDERS_*`, `PAYMENTS_*`, `GATEWAY_*`); cross-cutting codes (`INTERNAL`, `VALIDATION_FAILED`, `AUTH_UNAUTHENTICATED`, `AUTH_FORBIDDEN`) are owned and registered by `platform/apperr` itself.

### Registry process (adding a code)

1. Add the const to the owning package's codes block (`internal/domain/<agg>/codes.go`, or `internal/apperrs` for the gateway).
2. `apperr.Register(code, status, permanent, msgTemplate, params...)` in that package's `init()`. Duplicate registration panics at startup.
3. `just errgen` → regenerates [`docs/errors.md`](errors.md) from the live registry.
4. Commit the regenerated file — CI regenerates and fails on `git diff docs/errors.md`.

If the package is in a NEW service, blank-import the service's root package in `cmd/errgen/main.go` first (the root package transitively links the codes package).

The registry is **additive-only**: codes are never renamed or removed once shipped — clients switch on them. A different status or permanence is a new code, not an edit.

### Params rule (Google AIP-193)

Every `{placeholder}` in a message template must be declared in the registration's params, and the params travel verbatim to clients in the problem+json `params` member — so clients can build their own messages without parsing English. Enforced by a vet-style test over the registry (`TestRegistry_MessageTemplateInvariant` in `platform/apperr`). Params are stable API: add, never rename/remove/repurpose.

### Permanent semantics

`Permanent: true` means **no retry can succeed** (malformed payload, forbidden state transition). Messaging layers short-circuit on `apperr.IsPermanent`:

- `kafka.WithRetry` stops the in-process attempt loop on the first permanent failure and produces straight to the DLT;
- the tiered-retry escalator (`platform/messaging/retry`) skips every remaining tier and routes the record to `<topic>.DLT` directly.

Both stamp the `x-error-code` header with the apperr code so DLT triage can group by code (see `docs/operations.md` § DLT runbook).

### Wire shape (RFC 9457)

HTTP errors are `application/problem+json` (`httpx.Problem`): the standard members `type`/`title`/`status`/`detail`/`instance` plus the extension members that form the machine-readable contract:

```json
{
  "title": "Conflict",
  "status": 409,
  "detail": "order cannot transition from paid to created",
  "instance": "/v1/orders/1b4e28ba-…",
  "code": "ORDERS_INVALID_STATUS_TRANSITION",
  "params": {"from": "paid", "to": "created"}
}
```

`httpx.FromError` maps any `*apperr.Error` in the chain to its status/code/params; request-decode validation failures map to `VALIDATION_FAILED` with `params.fields = [{field, rule, param}]` (plus the legacy `errors` field-map); **anything else becomes a bare 500 `INTERNAL` — unknown error text is never leaked to clients**. Handlers answer errors with `httpx.WriteError(w, r, err)` and never construct problem bodies inline.

The full code catalog lives in [`docs/errors.md`](errors.md) (generated — see above).

---

## 11. Time

Everything is UTC, end to end. The rules:

| Concern | Rule | Where enforced |
|---|---|---|
| DB columns | `timestamptz` only, never `timestamp` | migrations (squawk-linted) |
| Row timestamps | DB time: `DEFAULT now()` set by SQL — transactional, consistent across replicas | table DDL; repositories ignore caller-supplied creation times |
| Scanning | pgx registers `pgtype.TimestamptzCodec{ScanLocation: time.UTC}` on every connection of BOTH pools — scanned `time.Time` values are UTC regardless of the session/server TZ | `platform/storage/pg` (config.go AfterConnect) |
| API responses | RFC 3339 UTC with the `Z` suffix, always (`created_at`) | gateway views (`.UTC().Format(time.RFC3339)`) |
| Display zones | `X-Timezone` request header (IANA name, tzdb-validated, else 400 `GATEWAY_INVALID_TIMEZONE`) adds a display-only `created_at_local`; the contract field stays UTC | gateway API |
| Business "now" | inject `platform/clock.Clock` ONLY where business logic reads the current time to decide something (payments' `occurred_at`); orders injects no clock at all | `platform/clock` godoc |
| Cutoffs/expiry | computed in SQL against the DATABASE clock (`now()`), never an app clock — every instance agrees regardless of host skew, and the compared column is DB time too | e.g. orders `ListUnpaidExpired` |
| Tests | `testing/synctest` bubbles make `time.Now` (and `clock.System`) fake and deterministic — prefer over fake-clock implementations | `platform/clock` tests |

**DST / civil dates (future seam).** UTC instants are unambiguous, so nothing in the current system cares about DST — `created_at_local` rendering just follows the tzdb offset for that instant. But a **civil date** (birthday, business day, "end of the local month") is *not* an instant: if a feature ever needs one, store it as date + IANA zone and convert at the edges — do not store a midnight UTC timestamp, which shifts by an hour across DST transitions.

---

## 12. i18n

**The API contract is `code` + `params`, and the client localizes.** Server-side localization of problem `title`/`detail` via `Accept-Language` is a courtesy for humans reading raw responses — clients must never parse those strings, and `code`/`params` are never localized.

Mechanics (`platform/i18n`, built on go-i18n v2 + `x/text` matching):

- **Message IDs** are apperr codes (`GATEWAY_ORDER_NOT_FOUND`), optional `<code>.title` title overrides, and `validation.<rule>` keys for per-field validator rules (`validation.required`, `validation.min`, …).
- **Catalogs are TOML** files embedded with the package that OWNS the codes: `platform/i18n/catalog/` ships en (base) + ru for the platform codes and the common validation rules; the gateway embeds en + ru for its `GATEWAY_*` codes in `internal/apperrs/catalog/` and merges them via `Bundle.Load` at startup. A service whose codes never surface on localized HTTP responses (orders' `ORDERS_*` consumer-side codes today) ships no catalog until they do.
- **Negotiation**: `i18n.Middleware` parses `Accept-Language` (q-weights honored, unsupported → en) and installs the locale + localizer into the request context.
- **The seam**: `httpx` stays free of i18n imports — it owns a `ProblemLocalizer` context key; the i18n middleware installs an implementation, and `httpx.WriteError` localizes title/detail when one is present, falling back to the registered developer message when the key is missing from the catalog. For `VALIDATION_FAILED` problems the same localizer is consulted per field with the `validation.<rule>` key (params: `field`/`rule`/`param` from `params.fields`), so the legacy `errors` map carries localized per-field messages; untranslated rules keep the English developer string.

---

## 13. Observability: duration metrics

**Every new RPC-ish path ships a duration histogram.** Anything that crosses a process or network boundary — an HTTP route, a CQRS handler, a Kafka handler or produce, a DB query, an outbox hop, a business state machine reaching its terminal state — records how long it took, as a **histogram** (never a gauge or a pair of counters: histograms are what p50/p95/p99 quantiles, heatmaps, and SLO burn rates are computed from). Naming and shape rules: instrument name `<area>.<thing>.duration` (e.g. `kafka.consumer.handler.duration`, `pg.query.duration`, `orders.lifecycle.duration`; lag-style end-to-end delays may use `<area>.<thing>_lag`), unit **seconds** (`metric.WithUnit("s")`, float values via `d.Seconds()`), and **low-cardinality labels only** — bounded enums like `{topic, status=ok|error}`, `{query, pool=writer|reader}`, `{terminal_status}`; never IDs, never raw SQL, never unbounded user input. Instruments are created from the global otel meter at constructor time and nil-degrade on creation failure (metrics must never break the path they measure) — see `platform/messaging/kafka/metrics.go` for the canonical pattern. Bucket layout is not configured at the instrument: the telemetry SDK applies exponential-histogram aggregation by view, so call sites stay layout-agnostic.

**Legacy exceptions (grandfathered):** `cqrs.handler.duration_ms` and `http.server.duration` predate the seconds rule and record **milliseconds**. They stay as-is — a metric's name+unit is part of the operational contract (dashboards, recording rules, alerts all encode it), so renaming or re-uniting them is a breaking metric change, not a cleanup. New instruments must follow the seconds rule above; do not copy these two.
