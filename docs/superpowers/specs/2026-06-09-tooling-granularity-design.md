# SP9: Tooling + File Granularity — Design

Status: approved (2026-06-09). Scope: file-granularity refactor, justfile (replace Taskfile), lefthook hooks, richer tooling (gofumpt/air/govulncheck/more-linters), dependabot + release CI.

## Goals
1. **File granularity** — split files that mix 2+ responsibilities or exceed ~200 LOC into focused single-responsibility files. Public API unchanged, all tests green. Document the convention.
2. **justfile** replaces `Taskfile.yml` — all targets migrated, param support, recipes for the full dev loop. Remove Taskfile.
3. **lefthook** — local pre-commit (fmt + lint changed + build) and pre-push (test -short) hooks.
4. **Tooling** — gofumpt+goimports formatters; air hot-reload; expanded golangci linter set (kept at 0 issues); govulncheck; dependabot; goreleaser release workflow.

## 1. File-granularity refactor (one responsibility per file)
**Convention** (documented in `docs/conventions.md`): a file holds one cohesive responsibility; named for the concept; a type + its constructor live together; distinct behaviors/sub-systems get their own file; config structs in `config.go`; HTTP handlers separate from wiring. Keep package APIs identical.

Targeted splits (verify exact current contents before splitting; keep imports tidy):
- `examples/internal/service/service.go` (373) → `service.go` (Service struct + New) · `config.go` (Config) · `admin.go` (admin server + health + /metrics mount) · `consumers.go` (AddConsumer + retry/DLT wrap) · `relay.go` (AddOutboxRelay) · `cleanup.go` (AddAuditCleanup + inbox cleanup launch) · `lifecycle.go` (Start/Stop/runCtx ordering) · `publisher.go` (DefaultOutboxPublisher).
- `examples/gateway/gateway.go` (363) → `app.go` (App struct + NewApp) · `config.go` · `routes.go` (mux + route mounting + auth/CORS/ratelimit wiring) · `deps.go` (optional blob/cache/featureflags/verifier builders with graceful degradation).
- `platform/health/health.go` (224) → `health.go` (Health struct + state) · `check.go` (Check / CheckFunc) · `handlers.go` (LivezHandler/ReadyzHandler/Mount).
- `platform/testkit/fakes/fakes.go` (247) → `cache.go` · `objectstore.go` · `publisher.go` · `verifier.go` (one fake each).
- `platform/cache/cache.go` (188) → `cache.go` (Cache struct + New + Get/Set) · `config.go` (Config + jitter) · `tiers.go` (L1 otter + L2 rueidis helpers) — or keep jitter in cache.go if tiny; split where it reads better.
- `platform/kafka/consumer.go` (206) → `consumer.go` (Consumer + NewConsumer + Close) · `run.go` (the poll/per-partition/commit loop).
- Borderline (split ONLY if 2+ clear concerns emerge on reading): `platform/telemetry/telemetry.go` (tracer vs meter setup), `platform/audit/audit.go` (Store/PgStore vs behavior), `platform/serde/schemaregistry.go` (client vs serde vs descriptor-printer), `platform/outbox/relay.go` (relay vs batch helpers). Use judgment; do not over-split tiny files.

DO NOT: change exported identifiers, move types between packages, or alter behavior. Each split is a pure file-move refactor — run the package's tests after each to confirm green.

## 2. justfile (replace Taskfile)
Create `justfile` at repo root mirroring + improving the Taskfile recipes, using just's parameters:
- `build`, `run svc=skeleton`, `dev svc=gateway` (air), `tidy`, `gen` (buf + sqlc + mocks), `gen-mocks`.
- `test pkg='./...'` (`go test {{pkg}}`), `test-unit` (`go test -short ./...`), `test-integration`, `test-e2e`, `test-cover`.
- `lint`, `lint-fix` (`golangci-lint run --fix`), `fmt` (gofumpt+goimports via golangci fmt or direct), `vuln` (govulncheck ./...), `audit` (fmt-check + lint + vuln + test-unit).
- `up`, `up-obs`, `up-apps`, `up-full`, `down`, `logs`, `ps`, `build-images`, `token`.
- `hooks` (`lefthook install`), `release` (goreleaser build/snapshot).
Remove `Taskfile.yml`. Update README + docs/operations.md + ci.yml comments to reference `just` (CI can still call the underlying `go`/`golangci-lint` directly — do not require `just` in CI runners unless trivial). Document installing `just`.

## 3. lefthook
`lefthook.yml`:
- `pre-commit`: parallel — (a) format: `gofumpt -w` + `goimports -w` on staged `*.go` (stage_fixed); (b) lint: `golangci-lint run` on changed packages; (c) `go build ./...`.
- `pre-push`: `go test -short ./...`.
Install: `just hooks` → `lefthook install` (writes `.git/hooks/*`). Document in README/CONTRIBUTING that contributors run `just hooks` once. Add `lefthook` install note (brew/go install). Keep hooks fast (only staged/changed files where possible).

## 4. Tooling
- **gofumpt + goimports**: add to golangci v2 `formatters` (`gofumpt`, `goimports`); run repo-wide once to normalize (commit the reformat). `just fmt` applies them.
- **air**: `.air.toml` (build `./examples/{{svc}}/cmd/{{svc}}` or `./cmd/skeleton`, watch `.go`, exclude tmp/gen/vendor). `just dev svc=gateway` runs air. Add air install note.
- **Richer golangci** (`.golangci.yml`, v2): add high-value linters on top of the existing 12: `gosec`, `gocritic`, `misspell`, `unconvert`, `prealloc`, `perfsprint`, `errname`, `nilerr`, `nilnil`, `wastedassign`, `usestdlibvars`, `copyloopvar`. Configure exclusions for `_test.go` (gosec G101/test noise) and generated files (already skipped). RUN it; FIX all new findings so `golangci-lint run` stays **0 issues**. If a specific linter produces low-value/unfixable noise across the codebase, drop it from the set and note why in a comment. govulncheck stays a CI job + `just vuln` locally.
- **Dependabot** `.github/dependabot.yml`: ecosystems `gomod` (weekly), `github-actions` (weekly), `docker` (weekly, for the Dockerfile base images). Sensible grouping/labels.
- **Release**: `.goreleaser.yaml` + `.github/workflows/release.yml` (trigger on tag `v*`): build the 4 service images (matrix or goreleaser `dockers`) multi-arch (amd64/arm64), generate SBOM (syft), sign with cosign (keyless OIDC), push to GHCR. Since the project has no remote yet, make the workflow correct + documented but it only runs on a tag push to a configured registry — keep registry/owner as `${{ github.repository_owner }}`/GHCR placeholders. Validate the goreleaser config (`goreleaser check`) if goreleaser is available; else ensure YAML validity.

## 5. CI/CD
- `ci.yml`: add a `format` step/job running `gofumpt -l .` (fail if any file needs formatting) — fast gate. Keep existing lint/buf/govulncheck/unit/test/build jobs. Reference the expanded linter config (no CI change needed beyond the config).
- Add `release.yml` + `dependabot.yml` as above.

## Out of scope
- Repackaging (no new packages / no moving types between packages — only file splits within existing packages). Renovate (using Dependabot). Actual registry push (no remote; workflow is correct-but-dormant).

## Verification
- After EACH package's file split: that package's tests pass (`-short` + integration where relevant). 
- Whole-repo: `go build ./...`, `go vet`, `gofumpt -l .` clean, `golangci-lint run` **0 issues** (with the expanded set), `go test -short ./...` green, e2e green, `go generate ./platform/testkit/mocks/...` reproducible.
- `just <recipe>` works for the key recipes; `Taskfile.yml` removed; `lefthook install` wires hooks; `docker compose config` (all profiles) valid; release/goreleaser config + dependabot + CI YAML valid.
- No exported-identifier changes (the split is API-preserving): a quick `go doc` / `go build ./...` of dependents confirms.
