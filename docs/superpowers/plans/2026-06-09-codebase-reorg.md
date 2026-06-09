# SP10: Codebase Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. PURE STRUCTURAL refactor — move dirs, rewrite import paths, NO API/behavior change. Package names stay identical (only import paths move). Keep `golangci-lint run` 0 + tests green + gofumpt clean after every task. macOS/darwin (BSD sed: `sed -i ''`). lefthook pre-commit runs build+lint on commit — keep green.

**Goal:** Group flat `platform/` packages into 5 domain dirs + 6 flat standalones; standardize example-service internals; relocate harness; update all import paths, codegen/config, docs.

**Architecture:** `git mv` package dirs into group dirs; global-replace the unique import-path strings; `goimports -w` to regroup; update codegen/config referencing moved dirs; build+test+commit per group (small blast radius).

**Spec:** `docs/superpowers/specs/2026-06-09-codebase-reorg-design.md`.

Mechanical procedure per package move (example for `kafka`→`messaging/kafka`):
```bash
mkdir -p platform/messaging
git mv platform/kafka platform/messaging/kafka
# rewrite the import path everywhere (quoted unique string):
grep -rl '"go-boilerplate/platform/kafka' --include='*.go' . \
  | xargs sed -i '' 's#go-boilerplate/platform/kafka#go-boilerplate/platform/messaging/kafka#g'
```
Then `goimports -w .` (regroup), `go build ./...`, `go test -short ./...`, `golangci-lint run ./...`.
NOTE on prefix overlap: `platform/outbox` is a prefix of `platform/outboxkafka`. Both move to `messaging/`, so the replace `platform/outbox#platform/messaging/outbox` rewrites BOTH correctly (`outboxkafka`→`messaging/outboxkafka`, `outbox`→`messaging/outbox`). Still, after replacing, GREP to confirm no stale `"go-boilerplate/platform/outbox"` (without `messaging/`) remains and no double-`messaging/messaging`.

---

## Task 1: messaging group
Move `kafka, serde, outbox, inbox, outboxkafka` → `platform/messaging/`.
- [ ] `mkdir -p platform/messaging`; `git mv` each of the 5 dirs into `platform/messaging/`.
- [ ] Global import-path replace for each (quoted-string sed as above): `platform/kafka`→`platform/messaging/kafka`, `platform/serde`→`platform/messaging/serde`, `platform/outboxkafka`→`platform/messaging/outboxkafka` (do this BEFORE outbox to avoid overlap ambiguity), `platform/outbox`→`platform/messaging/outbox`, `platform/inbox`→`platform/messaging/inbox`.
- [ ] `goimports -w .`
- [ ] Verify: `grep -rn '"go-boilerplate/platform/\(kafka\|serde\|outbox\|inbox\|outboxkafka\)"' --include='*.go' .` → EMPTY (no stale paths); `grep -rn 'messaging/messaging' .` → EMPTY.
- [ ] `go build ./... && golangci-lint run ./... 2>&1 | tail -1 && gofumpt -l $(git ls-files '*.go'|xargs grep -L '^// Code generated')` (0 issues, no unformatted).
- [ ] `go test -short ./...` green. Integration for messaging (Docker): `go test ./platform/messaging/... 2>&1 | grep -E 'ok|FAIL'`.
- [ ] NOTE: `platform/messaging/outbox/sqlc.yaml` moved with the dir; its paths are relative → unaffected. The mocks `//go:generate ... ../../outbox Publisher` directive is now stale but not executed yet (fixed in Task 7); the generated `publisher_mock.go` import was rewritten by the replace → still compiles. Confirm `go build ./platform/testkit/...` ok.
- [ ] Commit: `refactor(platform): group messaging packages (kafka/serde/outbox/inbox/outboxkafka)`.

## Task 2: observability group
Move `log, telemetry, health` → `platform/observability/`.
- [ ] `mkdir -p platform/observability`; `git mv` the 3 dirs.
- [ ] Replace `platform/log`→`platform/observability/log`, `platform/telemetry`→`…/observability/telemetry`, `platform/health`→`…/observability/health`. (Careful: `platform/log` could prefix-collide with nothing else — but verify no `platform/login`-style false hits; there are none.) `goimports -w .`
- [ ] Verify no stale paths (grep) + no double-path.
- [ ] `go build ./... && golangci-lint run ./...` 0 + gofumpt clean + `go test -short ./...` green + integration `go test ./platform/observability/...`.
- [ ] Commit: `refactor(platform): group observability packages (log/telemetry/health)`.

## Task 3: web group
Move `httpserver, httpx` → `platform/web/`.
- [ ] `mkdir -p platform/web`; `git mv` both.
- [ ] Replace `platform/httpserver`→`platform/web/httpserver`, `platform/httpx`→`platform/web/httpx`. `goimports -w .`
- [ ] Verify + `go build` + lint 0 + gofumpt + `go test -short ./...` + integration `go test ./platform/web/...`.
- [ ] Commit: `refactor(platform): group web packages (httpserver/httpx)`.

## Task 4: security group
Move `auth, authz, audit` → `platform/security/`.
- [ ] `mkdir -p platform/security`; `git mv` the 3 dirs. (audit has a `migrations/` subdir → moves with it; embed path relative → fine.)
- [ ] Replace `platform/authz`→`platform/security/authz` FIRST (authz is prefix-overlap with auth), then `platform/auth`→`platform/security/auth`, then `platform/audit`→`platform/security/audit`. `goimports -w .`
- [ ] Verify no stale + no `security/security`. The mocks directives `../../auth Verifier` + `../../audit Store` are now stale (fixed Task 7); generated mocks' imports rewritten → compile.
- [ ] `go build` + lint 0 + gofumpt + `go test -short ./...` + integration `go test ./platform/security/...`.
- [ ] Commit: `refactor(platform): group security packages (auth/authz/audit)`.

## Task 5: storage group
Move `pg, cache, blob` → `platform/storage/`.
- [ ] `mkdir -p platform/storage`; `git mv` the 3 dirs.
- [ ] Replace `platform/pg`→`platform/storage/pg`, `platform/cache`→`platform/storage/cache`, `platform/blob`→`platform/storage/blob`. `goimports -w .`
- [ ] Verify + `go build` + lint 0 + gofumpt + `go test -short ./...` + integration `go test ./platform/storage/...` (Docker: pg/cache/blob testcontainers).
- [ ] The mocks directive `../../blob ObjectStore` now stale (Task 7). Generated mock import rewritten → compiles.
- [ ] Commit: `refactor(platform): group storage packages (pg/cache/blob)`.

## Task 6: harness rename + example-service consistency
- [ ] Move `examples/internal/service` → `examples/servicekit/`: `mkdir -p examples/servicekit`-not-needed (`git mv examples/internal/service examples/servicekit`). Remove now-empty `examples/internal/` if nothing else there.
- [ ] Rename the package `service` → `servicekit`: edit the `package service` declarations → `package servicekit` in the moved files; update import path `go-boilerplate/examples/internal/service`→`go-boilerplate/examples/servicekit` everywhere; update all references `service.New`→`servicekit.New`, `service.Config`→`servicekit.Config`, `service.Service`→`servicekit.Service`, etc. in the 4 services + e2e (`grep -rn '\bservice\.' examples/ | grep -v servicekit` to find them — but BEWARE other local vars named `service`; only the package-qualified refs to the harness package change). `goimports -w .`
- [ ] Example-service consistency: ensure the Kafka-inbound code is under `internal/transport/` in all CQRS services (orders/payments already; if gateway's `projection` consumer or any service deviates, leave gateway `projection`/`api`/`attachments` as role-specific — only fix genuine inconsistency, do NOT invent empty dirs). Document the standard shape (done in Task 8). If nothing needs renaming, note "already consistent".
- [ ] Verify: `grep -rn 'examples/internal/service' .` → EMPTY; `go build ./... && golangci-lint run ./...` 0 + gofumpt + `go test -short ./...` green.
- [ ] Commit: `refactor(examples): rename harness service→servicekit (surfaced from internal/), standardize layout`.

## Task 7: codegen / config / CI sweep + regenerate
- [ ] `platform/testkit/mocks/gen.go` — update the `//go:generate` source dirs to the new relative paths: `../../outbox`→`../../messaging/outbox`, `../../cqrs` (unchanged), `../../auth`→`../../security/auth`, `../../blob`→`../../storage/blob`, `../../audit`→`../../security/audit`. Run `go generate ./platform/testkit/mocks/...`; `git diff` should show NO change to the generated `*_mock.go` (imports already rewritten in earlier tasks) → confirms reproducible. If a regenerated file differs, investigate.
- [ ] `justfile` — the `gen` recipe references sqlc dirs / `go generate` paths: update any `platform/outbox` (→`platform/messaging/outbox`) sqlc path; confirm `gen-mocks` path (`./platform/testkit/mocks/...` unchanged). Run `just gen` (or the sqlc + buf + mocks steps) and confirm the tree builds + no unexpected diff.
- [ ] `sqlc generate` for `platform/messaging/outbox` (cd into it or via config) — confirm regenerated `gen/*.go` matches (no diff beyond the move).
- [ ] `.github/workflows/ci.yml` — grep for any moved path (`platform/outbox`, `platform/kafka`, etc.) in job steps/globs; update. The gofumpt gate uses `git ls-files` (path-agnostic) → fine.
- [ ] Locate the platform-never-imports-examples enforcement (search: `grep -rn 'examples' --include='*_test.go' platform cmd; grep -rn 'go list -deps' .`). If it exists, confirm it still works with the new paths (it greps deps for `/examples`, platform-path-agnostic → likely fine); update if it hardcodes old platform paths.
- [ ] Verify: `go build ./... && go test -short ./... && golangci-lint run ./...` 0; `go generate ./platform/testkit/mocks/...` reproducible (no diff); `sqlc generate` no diff; `docker compose config` valid.
- [ ] Commit: `chore(codegen): update mocks/sqlc/ci paths after platform regroup; regenerate (reproducible)`.

## Task 8: docs
- [ ] `platform/README.md` (new): index — one line per group (messaging/observability/web/security/storage) listing its packages + purpose, and the flat standalones (cqrs/resilience/config/run/featureflags/testkit).
- [ ] `docs/conventions.md` — update the file-org section with the new platform group map + the standard example-service internal layout (app/transport/store/migrations + edge extras).
- [ ] `docs/ARCHITECTURE.md`, `docs/adding-a-service.md`, `README.md` (layout table), `plan.md` (§3 layout if it lists packages) — update package paths to the new tree.
- [ ] Verify: `grep -rn 'platform/kafka\b\|platform/pg\b\|platform/httpserver\b\|examples/internal/service' README.md docs/ plan.md` → no stale references (all now grouped paths).
- [ ] Commit: `docs: update layout to grouped platform tree + standard service layout`.

## Task 9: final verify + adversarial review
- [ ] Whole-repo: `go build ./...`, `go vet ./...`, `gofumpt -l` (non-generated empty), `golangci-lint run ./...` 0, `go test -short ./...` green, `go generate ./platform/testkit/mocks/...` reproducible, `docker compose config` (all profiles) valid, `just --list` works.
- [ ] Integration + e2e (Docker): `go test ./platform/... ./examples/...` (or key integration pkgs) + `go test -count=1 -timeout 300s ./examples/e2e/...` → **e2e green**.
- [ ] No stale old import paths anywhere: `grep -rn '"go-boilerplate/platform/\(kafka\|serde\|outbox\|outboxkafka\|inbox\|log\|telemetry\|health\|httpserver\|httpx\|auth\|authz\|audit\|pg\|cache\|blob\)"' --include='*.go' .` → EMPTY. `grep -rn 'examples/internal/service' .` → EMPTY.
- [ ] No exported API changed (the refactor is path-only): spot-check `go doc` of a few moved packages shows the same symbols.
- [ ] Dispatch a final adversarial review (no broken imports, no behavior drift, tree is genuinely clearer, codegen reproducible, docs accurate). Fix findings.

---

## Self-Review (completed)
- **Spec coverage:** platform grouping (T1-5) · harness+examples (T6) · codegen/config/CI (T7) · docs (T8) · verify (T9). All spec sections mapped.
- **Placeholders:** none; exact `git mv` + sed mappings + per-task verification given. Prefix-overlap hazards (outbox/outboxkafka, auth/authz) called out with ordering.
- **Consistency:** every task ends build+lint-0+gofumpt+test green; import-path mappings consistent across tasks; mocks-directive fix deferred to T7 with rationale (generated imports already rewritten per-group so compilation holds throughout).
