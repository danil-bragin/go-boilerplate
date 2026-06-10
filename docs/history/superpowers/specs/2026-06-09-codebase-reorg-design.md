# SP10: Codebase Reorganization — Design

Status: approved (2026-06-09). Scope: group the flat `platform/` packages into cohesive domain subdirectories, standardize example-service internals, relocate the harness, update all import paths + codegen/config/docs. Pure structural refactor — no API or behavior change.

## Goal
Make the tree navigable: 21 flat `platform/` siblings → 5 domain groups + 6 standalones (≤2 levels); consistent example-service internals; harness surfaced. Same code, same exported APIs, tests green.

## Research basis
Go favors shallow (1–2 level) hierarchies + package-by-feature; `golang-standards/project-layout` is not official and often overkill; descriptive names, no `utils/common`. 21 flat siblings is idiomatic but past the comfortable scan limit → domain grouping (still ≤2 levels) is justified. `platform/` stays top-level (it is the reusable "library half" the boilerplate user copies; the platform-never-imports-examples rule remains).

## 1. Target platform/ layout
Package NAMES are unchanged (`package kafka`, `package pg`, …) — only the directory path (hence import path) moves. Call sites are unaffected (`kafka.NewConsumer` stays); only `import` lines change.

```
platform/
  messaging/     kafka · serde · outbox · inbox · outboxkafka
  observability/ log · telemetry · health
  web/           httpserver · httpx
  security/      auth · authz · audit
  storage/       pg · cache · blob
  cqrs/  resilience/  config/  run/  featureflags/  testkit/   (stay flat — genuine standalones)
```
Import path mapping (each is a unique string → safe global replace):
- `go-boilerplate/platform/kafka` → `…/platform/messaging/kafka`; same for serde/outbox/inbox/outboxkafka.
- `…/platform/log|telemetry|health` → `…/platform/observability/<x>`.
- `…/platform/httpserver|httpx` → `…/platform/web/<x>`.
- `…/platform/auth|authz|audit` → `…/platform/security/<x>`.
- `…/platform/pg|cache|blob` → `…/platform/storage/<x>`.
- cqrs/resilience/config/run/featureflags/testkit unchanged.

No package-name collisions (each dir keeps its unique name; imports are by path). Embedded migrations (`//go:embed migrations/*.sql` in audit, inbox) use relative paths → unaffected by the move; the `migrations/` subdir moves with its package.

## 2. Example services — consistent internals + harness move
- Standard service internal layout (documented in docs/conventions.md + docs/adding-a-service.md):
  - `internal/app/` — CQRS command/query handlers.
  - `internal/transport/` — Kafka inbound adapters (consumers/handlers).
  - `internal/store/` — sqlc (`gen/` + `queries/`).
  - `internal/migrations/sql/` — goose migrations.
  - Edge/role-specific extras are allowed and stay where they belong: gateway `internal/api` (generated REST), `internal/attachments` (feature), `internal/projection` (read-model consumer); notifications keeps its terminal shape (transport + inbox, no forced empty `app/`).
- Make naming consistent across the 3 CQRS services (orders/payments/gateway use the same names for the same roles). Only RENAME for consistency where a service deviates without reason; do NOT invent empty packages.
- Harness: `examples/internal/service` → `examples/servicekit/`, package renamed `service` → `servicekit`. Update all references (`service.New`→`servicekit.New`, `service.Config`→`servicekit.Config`, etc.) in the 4 services + e2e.

## 3. Config / codegen / CI path updates (must move with the dirs)
- `sqlc.yaml` (and any per-service sqlc config) — output/query dirs for any moved package that uses sqlc (e.g. `platform/outbox` → `platform/messaging/outbox`). Update + re-run `sqlc generate`; confirm no diff beyond paths.
- `buf.gen.yaml` / buf config — if any output path references a moved dir (proto gen is under `gen/`, likely unaffected — verify).
- `justfile` `gen`/`gen-mocks` recipes — paths to sqlc dirs + `go generate ./platform/testkit/mocks/...` (testkit unmoved → fine) + the mocks `//go:generate` source dirs (`../../outbox` etc. → `../../messaging/outbox`). Update the `//go:generate` directives in `platform/testkit/mocks/gen.go`.
- `.github/workflows/ci.yml` — any path globs referencing moved dirs (the platform-imports-examples check, buf paths). Update.
- The platform-never-imports-examples enforcement (`go list -deps` check, wherever it lives) — confirm still correct (it greps for `examples`, path-agnostic for platform, so likely fine; verify).
- testkit moq mocks: regenerate after the `//go:generate` source-path update; confirm reproducible.

## 4. Docs
Update to the new tree: `docs/ARCHITECTURE.md`, `docs/conventions.md` (the file-org + the new platform group map + standard service layout), `docs/adding-a-service.md`, `README.md` (layout table), `plan.md` (layout §3 if it lists packages), any ADR referencing package paths. Add a `platform/README.md` index (one line per group → what lives there).

## Execution strategy (group-by-group, small blast radius)
One package-group per task. For each: `git mv` the dirs into the group → global replace the old import-path strings with the new ones (unique strings) → `goimports -w` to regroup imports → update any codegen/config referencing the moved dirs → `go build ./... && go test -short ./... && golangci-lint run ./...` → integration/e2e where relevant → commit. Order: messaging, observability, web, security, storage, then servicekit-rename + example-consistency, then config/codegen sweep, then docs, then final verify + review. Keep gofumpt/lint 0 + tests green after every task.

## Out of scope
No API/behavior changes. No new packages beyond the group dirs. No moving types between packages. No platform→internal/ move (stays top-level). No example-service rewrites — only renames/moves for consistency.

## Verification
- After each group: `go build ./...`, `go test -short ./...`, `golangci-lint run` 0, `gofumpt -l` clean.
- Whole-repo final: build/vet/lint 0, fast lane green, **e2e green**, all integration tests green (Docker), `go generate ./platform/testkit/mocks/...` reproducible, `sqlc generate` no diff, `docker compose config` (all profiles) valid, `just --list` works, no exported API changed (`go doc` spot-check), no dangling old import paths (`grep -rn 'platform/kafka\b'` etc. returns nothing).
