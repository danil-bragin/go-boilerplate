# Contributing to go-boilerplate

## Development setup

### Prerequisites

| Tool | Install | Notes |
|---|---|---|
| Go 1.25+ | https://go.dev/dl/ | Required |
| Docker | https://docs.docker.com/get-docker/ | Required for integration tests + local stack |
| [just](https://just.systems/) | `brew install just` / `cargo install just` | Required for dev recipes |
| [lefthook](https://lefthook.dev/) | `brew install lefthook` | Required for git hooks |
| [golangci-lint](https://golangci-lint.run/usage/install/) | `brew install golangci-lint` | Required for pre-commit lint |
| air (optional) | `go install github.com/air-verse/air@latest` | Hot-reload (`just dev <svc>`) |

`gofumpt` and `goimports` are **not** required locally — the hooks invoke them via `go run` (e.g., `go run mvdan.cc/gofumpt@latest`) so no separate install is needed.

### Clone and install hooks

```bash
git clone <repo-url>
cd go-boilerplate

# Install lefthook git hooks (one-time setup)
just hooks
```

That's it. From that point on, hooks run automatically on every `git commit` and `git push`.

## Git hooks (lefthook)

Managed via `lefthook.yml` in the repo root. Install once with `just hooks` (`lefthook install`).

### pre-commit — runs on every `git commit`

| Step | Command | What it does |
|---|---|---|
| `fmt` | `go run mvdan.cc/gofumpt@latest` + `go run golang.org/x/tools/cmd/goimports@latest` | Auto-formats staged `.go` files; re-stages the changes (`stage_fixed: true`) |
| `lint` | `golangci-lint run ./...` | Lints the full repo (cached; fast on repeated runs) |
| `build` | `go build ./...` | Ensures the repo compiles |

If `fmt` modifies files, they are automatically re-staged before the commit lands — you never commit unformatted code.

### pre-push — runs on every `git push`

| Step | Command | What it does |
|---|---|---|
| `test` | `go test -short ./...` | Runs unit + functional tests (skips integration tests that require Docker) |

Integration tests (testcontainers-go, requires Docker) run in CI and via `just test-integration` locally.

### Running hooks manually

```bash
# Re-run pre-commit against the current staged files
lefthook run pre-commit

# Re-run pre-push
lefthook run pre-push

# Skip hooks for a single commit (use sparingly)
git commit --no-verify -m "wip: ..."
```

## Testing layers

See [`docs/testing.md`](docs/testing.md) for the full testing strategy, including:
- Unit tests (no external deps, `-short` flag)
- Functional / in-process tests
- Integration tests (testcontainers-go)
- End-to-end tests (`examples/e2e/`)

Quick reference:

```bash
just test-unit          # fast lane — unit + functional only (no Docker)
just test-integration   # full lane — all tests (requires Docker)
just test-cover         # unit tests + coverage summary
```

## File organisation conventions

See [`docs/conventions.md`](docs/conventions.md) for the file-organisation and naming conventions followed in this repo (package structure, layer boundaries, naming rules, import order, tooling map).

## Useful `just` recipes

```bash
just build          # go build ./...
just lint           # golangci-lint run ./...
just fmt            # golangci-lint fmt ./...
just audit          # fmt + lint + vuln + unit tests
just hooks          # (re)install lefthook git hooks
just up             # start core infra via docker compose
just test           # run all tests
```

Run `just` (no args) to list all available recipes.
