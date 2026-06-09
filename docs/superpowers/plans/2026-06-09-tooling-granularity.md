# SP9: Tooling + File Granularity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Mature repo — read existing code/patterns; verify tool versions; keep public APIs identical for refactors; keep `golangci-lint run` at 0 issues and tests green. Report deviations.

**Goal:** Replace Taskfile with a justfile, add lefthook + gofumpt + air + richer linters + govulncheck + dependabot + goreleaser release, and refactor large/multi-concern files into focused single-responsibility files (API-preserving).

**Tech Stack:** just · lefthook · gofumpt/goimports · air · golangci-lint v2 (expanded) · govulncheck · Dependabot · goreleaser + cosign/syft.

**Spec:** `docs/superpowers/specs/2026-06-09-tooling-granularity-design.md`.

Order matters: formatting/linting first (so later splits are already conformant), then runners/hooks/CI, then file splits, then docs.

---

## Task 1: gofumpt/goimports formatters + repo-wide reformat
**Files:** modify `.golangci.yml`; reformat tree.
- [ ] Add a `formatters:` block to `.golangci.yml` (v2) enabling `gofumpt` and `goimports` (keep existing `linters:`). Verify v2 formatters schema: `golangci-lint help` / docs; the v2 config has a top-level `formatters: { enable: [gofumpt, goimports] }`.
- [ ] Run `golangci-lint fmt ./...` (v2 formatter run) OR `gofumpt -w .` + `goimports -w .` over the tree (exclude `gen/`, `*.pb.go`, `*_mock.go` — they have generated headers; gofumpt on them is harmless but skip if it churns). Confirm `gofumpt -l .` reports no non-generated files.
- [ ] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run ./...` 0 issues, `go test -short ./...` green.
- [ ] Commit `style: adopt gofumpt+goimports formatters and reformat repo`.

## Task 2: expand golangci linters + fix findings to 0
**Files:** modify `.golangci.yml`; fix code as needed across packages.
- [ ] Add to the enabled linters (on top of the existing set): `gosec`, `gocritic`, `misspell`, `unconvert`, `prealloc`, `perfsprint`, `errname`, `nilerr`, `nilnil`, `wastedassign`, `usestdlibvars`, `copyloopvar`. Add exclusion rules: for `_test.go` exclude `gosec` (test fixtures), keep generated files skipped. Configure `gocritic`/`gosec` to a sensible default ruleset (don't enable every opinionated check).
- [ ] Run `golangci-lint run ./...`; FIX every new finding (real fixes, not blanket nolint — use targeted `//nolint:<linter> // reason` only where the finding is a deliberate false positive). If a linter floods low-value noise across the codebase and isn't worth fixing, REMOVE it from the set and add a one-line comment in `.golangci.yml` explaining why. End state: `golangci-lint run ./...` = **0 issues**.
- [ ] Verify build + `go test -short ./...` still green (fixes didn't break behavior). 
- [ ] Commit `chore(lint): expand linter set (gosec/gocritic/misspell/...) and fix findings`.

## Task 3: justfile (replace Taskfile) + air
**Files:** create `justfile`, `.air.toml`; delete `Taskfile.yml`; update `ci.yml` comments/README references.
- [ ] Read `Taskfile.yml` for the current recipes. Create `justfile` with recipes (use just params + `set shell`): `default` (list), `build`, `run svc='skeleton'`, `dev svc='gateway'` (runs `air` with the svc), `tidy`, `gen` (buf generate + sqlc generate dirs + `go generate ./platform/testkit/mocks/...`), `gen-mocks`, `test pkg='./...'`, `test-unit` (`go test -short ./...`), `test-integration` (`go test ./...`), `test-e2e`, `test-cover`, `lint`, `lint-fix` (`golangci-lint run --fix`), `fmt` (`golangci-lint fmt ./...` or `gofumpt -w . && goimports -w .`), `vuln` (`go run golang.org/x/vuln/cmd/govulncheck@latest ./...`), `audit` (fmt-check + lint + vuln + test-unit), `up`, `up-obs`, `up-apps`, `up-full`, `down`, `logs`, `ps`, `build-images`, `token`, `hooks` (`lefthook install`), `release` (`goreleaser release --snapshot --clean` — guarded for local). Use `{{svc}}`/`{{pkg}}` params. Include doc comments per recipe (just shows them in `just --list`).
- [ ] Create `.air.toml`: build cmd `go build -o ./tmp/app ./examples/{{`— note air can't take just-params; instead make `dev` recipe pass the path via env/arg. Simpler: `.air.toml` builds a target set by an env var `AIR_MAIN` (air supports `[build] cmd` using a shell var) OR generate per-run. Pragmatic: the `dev` recipe does `air -build.cmd "go build -o ./tmp/app ./examples/{{svc}}/cmd/{{svc}}" -build.bin ./tmp/app` (air CLI flags), so `.air.toml` only needs watch/exclude defaults (exclude `tmp gen vendor`, include `.go`). Verify air flag names (`air -h`); adapt.
- [ ] Delete `Taskfile.yml`. Grep the repo for `task ` references (README, docs, ci.yml) and update them to `just` equivalents (CI should call `go`/`golangci-lint` directly — do NOT require installing `just` on CI runners; only docs/local use `just`).
- [ ] Verify: `just --list` works; `just test-unit` runs green & fast; `just lint` clean; `just fmt` no-ops on the formatted tree; `go build ./...` clean. (air `dev` is interactive — just confirm the recipe parses, don't run a long-lived process.)
- [ ] Commit `build: replace Taskfile with justfile (param-friendly) + air dev hot-reload`.

## Task 4: lefthook
**Files:** create `lefthook.yml`; update README/CONTRIBUTING.
- [ ] Create `lefthook.yml`: 
  - `pre-commit`: commands — `fmt` (`gofumpt -w {staged_files}` + restage via `stage_fixed: true`, glob `*.go`), `imports` (`goimports -w {staged_files}` similar), `lint` (`golangci-lint run` on the changed packages — use `{staged_files}` mapped to dirs, or run `golangci-lint run` repo-wide if simpler/fast enough — prefer changed for speed), `build` (`go build ./...`). 
  - `pre-push`: `test` (`go test -short ./...`).
  Verify lefthook.yml schema against the installed/declared lefthook version (`lefthook version` if installed; else write per the documented v1 schema). Use `glob: "*.go"` + `run: gofumpt -w {staged_files}` + `stage_fixed: true`.
- [ ] Add a `CONTRIBUTING.md` (or a README section) telling contributors to run `just hooks` once (installs lefthook git hooks) and that lefthook auto-formats+lints on commit. Add a lefthook install note (`brew install lefthook` / `go install github.com/evilmartians/lefthook@latest`).
- [ ] Verify: `lefthook.yml` is valid (if lefthook installed: `lefthook validate` or `lefthook run pre-commit` on a dummy; else YAML-parse it). Do NOT require lefthook installed in CI.
- [ ] Commit `build(hooks): lefthook pre-commit (fmt+lint+build) and pre-push (test -short)`.

## Task 5: dependabot + release (goreleaser) + CI format gate
**Files:** create `.github/dependabot.yml`, `.goreleaser.yaml`, `.github/workflows/release.yml`; modify `.github/workflows/ci.yml`.
- [ ] `.github/dependabot.yml`: `version: 2`; updates for `gomod` (directory `/`, weekly, grouped minor/patch), `github-actions` (`/`, weekly), `docker` (`/`, weekly — the Dockerfile). Labels `dependencies`.
- [ ] `.goreleaser.yaml`: build the 4 service binaries from `./examples/<svc>/cmd/<svc>` (and `./cmd/skeleton`) for linux amd64+arm64 (`CGO_ENABLED=0`); `dockers`/`docker_manifests` to build+push multi-arch images to `ghcr.io/${{ env or owner }}/<svc>` using the existing parametric `Dockerfile` (or per-binary); `sboms` (syft); `signs`/`docker_signs` via cosign keyless. Use placeholders for the registry owner. Validate with `goreleaser check` if goreleaser is available; else ensure schema-valid YAML (goreleaser v2 config).
- [ ] `.github/workflows/release.yml`: trigger `on: push: tags: ['v*']`; permissions `contents: write, packages: write, id-token: write`; checkout, setup-go, install goreleaser (`goreleaser/goreleaser-action@v6`), `cosign-installer`, `syft` (or rely on goreleaser), login to GHCR, run `goreleaser release --clean`. Document it's dormant until a remote + tag exist.
- [ ] `.github/workflows/ci.yml`: add a `format` job (setup-go, install gofumpt, run `test -z "$(gofumpt -l .)"` — fail if unformatted). Keep all existing jobs.
- [ ] Verify: all YAML valid (`python3 -c 'import yaml,glob; [yaml.safe_load(open(f)) for f in [".github/dependabot.yml",".goreleaser.yaml",".github/workflows/release.yml",".github/workflows/ci.yml"]]; print("yaml-valid")'`). `goreleaser check` if available.
- [ ] Commit `ci: dependabot, goreleaser release workflow, and gofumpt format gate`.

## Task 6: file splits — platform packages (API-preserving)
**Files:** split within `platform/health`, `platform/testkit/fakes`, `platform/cache`, `platform/kafka`; borderline `telemetry`/`audit`/`serde`/`outbox` only where 2+ concerns.
- [ ] For EACH target file: move (not rewrite) cohesive groups into new files in the SAME package. Do NOT change exported identifiers, signatures, or behavior. Per the spec §1:
  - `platform/health/health.go` → `health.go` (struct+state+New) · `check.go` (Check/CheckFunc) · `handlers.go` (Livez/Readyz/Mount).
  - `platform/testkit/fakes/fakes.go` → `cache.go`/`objectstore.go`/`publisher.go`/`verifier.go`.
  - `platform/cache/cache.go` → `cache.go` (Cache+New+Get/Set) · `config.go` (Config+jitter) · `tiers.go` (L1/L2 helpers) — split where it improves readability.
  - `platform/kafka/consumer.go` → `consumer.go` (type+NewConsumer+Close) · `run.go` (poll/partition/commit loop).
  - Borderline: read `telemetry.go`, `audit.go`, `serde/schemaregistry.go`, `outbox/relay.go`; split into 2 files each ONLY if there are 2 clearly separable concerns (e.g. telemetry: `tracer.go`+`meter.go`; serde: `client.go`+`serde.go`+`descriptor.go`). Use judgment; don't create 30-line files for the sake of it.
- [ ] After EACH package: `go build ./... && go test -short ./<pkg>/... && golangci-lint run ./<pkg>/...` green. Run the package's integration test too if it has containers (at least once at the end).
- [ ] Verify whole-repo: `go build ./...`, `go vet`, `golangci-lint run` 0 issues, `go test -short ./...` green. Confirm NO exported API changed (`go build ./...` of all dependents passes; spot-check `go doc` of a split package shows the same symbols).
- [ ] Commit `refactor(platform): split large files into single-responsibility files (API-preserving)`.

## Task 7: file splits — example services (API-preserving)
**Files:** split `examples/internal/service/service.go` and `examples/gateway/gateway.go` per spec §1.
- [ ] `examples/internal/service/service.go` → `service.go` · `config.go` · `admin.go` · `consumers.go` · `relay.go` · `cleanup.go` · `lifecycle.go` · `publisher.go` (move cohesive method groups; keep the `Service` type + all method sets identical).
- [ ] `examples/gateway/gateway.go` → `app.go` (App+NewApp) · `config.go` · `routes.go` (mux/route/auth/CORS/ratelimit/attachments mounting) · `deps.go` (optional blob/cache/featureflags/verifier builders). Keep `package gateway` exported API identical (NewApp, App methods, options).
- [ ] After each: `go build ./... && go test -short ./examples/...` green. Run the gateway + e2e + orders/payments/notifications tests (with Docker) to confirm no behavior change.
- [ ] Verify: e2e green, gateway tests green, `golangci-lint run ./examples/...` 0 issues.
- [ ] Commit `refactor(examples): split service-harness and gateway into focused files`.

## Task 8: docs — conventions + tooling references
**Files:** create `docs/conventions.md`; update `README.md`, `docs/operations.md`, `docs/testing.md`.
- [ ] `docs/conventions.md`: the file-organization convention (one responsibility per file; type+constructor together; config.go; handlers separate; naming), the tooling map (just recipes table, lefthook hooks, gofumpt/goimports, air, golangci linter set, govulncheck, dependabot, goreleaser), and "how to set up your dev env" (`just hooks`, install just/lefthook/air/gofumpt).
- [ ] Update README quickstart + any `task ...` references → `just ...`. Update `docs/operations.md` profile/up commands to `just up*`. Update `docs/testing.md` task references to `just test*`.
- [ ] Verify: no dangling `task ` references remain (`grep -rn 'task ' README.md docs/ | grep -v justfile`), markdown sane.
- [ ] Commit `docs: file-organization conventions + just/lefthook/tooling guide; replace task refs with just`.

## Task 9: final verify + adversarial review
- [ ] Whole-repo: `go build ./...`, `go vet ./...`, `gofumpt -l .` (empty), `golangci-lint run ./...` (0, expanded set), `go test -short ./...` (fast green), `go generate ./platform/testkit/mocks/...` reproducible, `docker compose config` (all profiles), all CI/goreleaser/dependabot YAML valid, `just --list` works, `Taskfile.yml` gone, `lefthook.yml` valid.
- [ ] Run integration + e2e (Docker): `go test ./examples/... ./platform/...` (or the key integration pkgs) to confirm the file splits didn't change behavior.
- [ ] Confirm NO exported API changed across the refactor.
- [ ] Dispatch a final adversarial review (file-granularity sanity, no API drift, lint/tooling correctness, hooks/release validity). Fix findings.

---

## Self-Review (completed)
- **Spec coverage:** formatters (T1) · linters (T2) · justfile+air (T3) · lefthook (T4) · dependabot+release+CI-gate (T5) · platform splits (T6) · example splits (T7) · docs (T8) · verify (T9). All spec sections mapped.
- **Placeholders:** none; tool-version-variance points (golangci v2 formatters schema, air flags, lefthook schema, goreleaser v2 schema) each carry "verify with -h/docs + adapt" instructions.
- **Consistency:** refactors are explicitly API-preserving (file-move only, tests green after each) — no exported-identifier changes anywhere. justfile recipe names consistent with docs (T8) + lefthook `hooks` recipe (T4). 0-issues-lint invariant maintained through T2 and re-checked in T6/T7/T9.
