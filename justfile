set shell := ["bash", "-uc"]

# Serial package execution for Docker-backed lanes: integration packages each
# start real containers (testcontainers), and running several such packages in
# parallel oversubscribes the Docker daemon (CI runners have ~4 vCPU) — the
# symptom is tests dying with exit code 133. Override per-invocation:
#   GOTESTFLAGS="" just test-integration
GOTESTFLAGS := env("GOTESTFLAGS", "-p 1")

# List all available recipes
default:
    @just --list

# ── Go development ─────────────────────────────────────────────────────────────

# Build all packages
build:
    go build ./...

# Run the skeleton service (cmd/skeleton)
run-skeleton:
    go run ./cmd/skeleton

# Run a named example service (e.g. just run gateway)
run svc='gateway':
    go run ./examples/{{svc}}/cmd/{{svc}}

# Hot-reload a service with air (e.g. just dev gateway); default: gateway
dev svc='gateway':
    air -build.cmd "go build -o ./tmp/app ./examples/{{svc}}/cmd/{{svc}}" -build.bin ./tmp/app

# Sync go.mod/go.sum
tidy:
    go mod tidy

# Run all code generators (buf + sqlc + oapi + mocks)
gen:
    buf generate
    find . -name 'sqlc.yaml' -not -path '*/node_modules/*' | xargs -n1 sqlc generate -f
    just oapi
    go generate ./platform/testkit/mocks/...
    goimports -w -local go-boilerplate platform/testkit/mocks/

# Regenerate docs/errors.md from the live apperr registry — CI fails when it is out of sync
errgen:
    go run ./cmd/errgen

# Regenerate the gateway HTTP server from openapi.yaml (oapi-codegen)
oapi:
    cd examples/gateway && oapi-codegen --config oapi.gen.yaml openapi.yaml

# Regenerate moq mocks for platform interfaces (goimports normalizes moq's import grouping)
gen-mocks:
    go generate ./platform/testkit/mocks/...
    goimports -w -local go-boilerplate platform/testkit/mocks/

# Run tests for a package or pattern (default: ./...) — serial packages (see GOTESTFLAGS)
test pkg='./...':
    go test {{GOTESTFLAGS}} {{pkg}}

# Fast lane: unit + functional tests only (-short, no Docker)
test-unit:
    go test -short ./...

# Full lane: all tests including integration (requires Docker) — serial packages (see GOTESTFLAGS)
test-integration:
    go test {{GOTESTFLAGS}} ./...

# Run the end-to-end choreography test (self-provisions Redpanda + Postgres via testcontainers; requires Docker, NOT docker compose)
test-e2e:
    go test ./examples/e2e/...

# Fast lane with coverage: outputs summary line to stdout
test-cover:
    go test -short -coverprofile=coverage.out ./...
    go tool cover -func=coverage.out | tail -1

# Fuzz the attacker-facing parsers (default 30s per target; e.g. just fuzz 5m)
fuzz fuzztime='30s':
    go test -fuzz=FuzzClientIPKey      -fuzztime={{fuzztime}} -run='^$' ./platform/web/httpserver
    go test -fuzz=FuzzHTTPXDecode      -fuzztime={{fuzztime}} -run='^$' ./platform/web/httpx
    go test -fuzz=FuzzParseRetryHeaders -fuzztime={{fuzztime}} -run='^$' ./platform/messaging/retry
    go test -fuzz=FuzzCursorDecode     -fuzztime={{fuzztime}} -run='^$' ./examples/gateway/internal/app

# ── Linting & formatting ───────────────────────────────────────────────────────

# Run golangci-lint
lint:
    golangci-lint run ./...

# Lint migration SQL with squawk (config: .squawk.toml) — same gate as CI
lint-sql:
    npx -y squawk-cli@latest $(git ls-files '*/migrations/*.sql' '*/migrations/sql/*.sql')

# Run golangci-lint with auto-fix
lint-fix:
    golangci-lint run --fix ./...

# Format code via golangci-lint fmt
fmt:
    golangci-lint fmt ./...

# Vulnerability scan (govulncheck)
vuln:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Full audit: fmt + lint + vuln + unit tests
audit:
    just fmt
    just lint
    just vuln
    just test-unit

# ── Docker Compose stack ───────────────────────────────────────────────────────

# Start core infra (pg, kafka, redis, seaweedfs, keycloak)
up:
    docker compose up -d

# Core + observability stack (otel-collector, jaeger, prometheus, grafana, pyroscope)
up-obs:
    docker compose --profile observability up -d

# Core + the 4 app services (gateway, orders, payments, notifications)
up-apps:
    docker compose --profile apps up -d --build

# Everything: core + observability + apps
up-full:
    docker compose --profile observability --profile apps up -d --build

# Stop everything and remove volumes
down:
    docker compose --profile observability --profile apps down -v

# Stream logs from all services
logs:
    docker compose --profile observability --profile apps logs -f

# Show running compose services
ps:
    docker compose ps

# Build Docker images for all four application services
build-images:
    docker build --build-arg SERVICE=gateway       -t go-boilerplate/gateway:local       .
    docker build --build-arg SERVICE=orders        -t go-boilerplate/orders:local        .
    docker build --build-arg SERVICE=payments      -t go-boilerplate/payments:local      .
    docker build --build-arg SERVICE=notifications -t go-boilerplate/notifications:local .

# ── Scaffolding ───────────────────────────────────────────────────────────────

# Scaffold a new example service from the payments template (e.g. just new-service shipping)
new-service name:
    ./scripts/new-service.sh {{name}}

# Rename the Go module path everywhere (e.g. just rename-module github.com/acme/myapp)
rename-module path:
    ./scripts/rename-module.sh {{path}}

# Dry-run of rename-module: print the planned changes, modify nothing
rename-module-check path:
    ./scripts/rename-module.sh --check {{path}}

# Smoke-test the scaffolding scripts (new-service build/vet + rename-module --check) — runs in CI
test-scaffold:
    ./scripts/test-scaffold.sh

# Compile/parse-check the Go code blocks in docs/adding-a-service.md — runs blocking in CI
doc-test:
    ./scripts/doc-test.sh

# ── Utilities ─────────────────────────────────────────────────────────────────

# Apply a service's embedded migrations (PG_DSN / PG_MIGRATE_URL from env; svc: gateway|orders|payments|notifications|all)
migrate svc:
    go run ./cmd/migrate -service {{svc}}

# Redrive DLT records back to their original topics (brokers default from KAFKA_BROKERS)
# Usage: just redrive --dlt orders.commands.DLT --dry-run
redrive *ARGS:
    go run ./cmd/redrive {{ARGS}}

# Fetch a Keycloak access token (demo/demo) for manual API calls — requires jq
# Usage: curl -H "Authorization: Bearer $(just token)" http://localhost:8080/v1/orders/<id>
token:
    curl -s -d client_id=gateway -d username=demo -d password=demo -d grant_type=password http://localhost:8180/realms/app/protocol/openid-connect/token | jq -r .access_token

# Fetch a machine-to-machine token (client-credentials, gateway-m2m service account) — requires jq
token-m2m:
    curl -s -d client_id=gateway-m2m -d client_secret=gateway-m2m-dev-secret -d grant_type=client_credentials http://localhost:8180/realms/app/protocol/openid-connect/token | jq -r .access_token

# Load test the gateway order flow via dockerized k6 (see docs/operations.md §Load testing)
# BASE_URL/TOKEN pass through; default BASE_URL targets the HOST's gateway from
# inside the k6 container. host.docker.internal resolves natively on macOS and
# Windows; the --add-host flag maps it to the host gateway IP on Linux too.
load vus='10' duration='30s':
    docker run --rm -i --add-host=host.docker.internal:host-gateway \
      -e BASE_URL="${BASE_URL:-http://host.docker.internal:8080}" \
      -e TOKEN="${TOKEN:-}" \
      grafana/k6 run --vus {{vus}} --duration {{duration}} - < scripts/k6/order-flow.js

# Install git hooks via lefthook
hooks:
    lefthook install

# Local snapshot release via goreleaser (requires goreleaser; snapshot = no publish)
release:
    goreleaser release --snapshot --clean

# ── PGO (profile-guided optimization) ─────────────────────────────────────────

# Fetch a CPU profile from Pyroscope into the service's main package as default.pgo.
# go build (1.21+) defaults to -pgo=auto: it picks up default.pgo from the main
# package dir automatically; when the file is absent builds are plain non-PGO.
# Degrades gracefully: if Pyroscope is down/empty, nothing is overwritten.
# Usage: just pgo-fetch gateway [addr] [window]   (addr default http://localhost:4040)
pgo-fetch svc addr='http://localhost:4040' window='24h':
    #!/usr/bin/env bash
    set -euo pipefail
    svc='{{svc}}'
    dir="examples/$svc/cmd/$svc"
    [[ -d "$dir" ]] || dir="cmd/$svc"
    [[ -d "$dir" ]] || { echo "pgo-fetch: no main package at examples/$svc/cmd/$svc or cmd/$svc" >&2; exit 1; }
    # Pyroscope render API: /pyroscope/render?query=<profile-type>{service_name="<app>"}&from=…&format=pprof
    # The app name is Telemetry.ServiceName (OTEL_SERVICE_NAME), set by servicekit's pyroscope-go wiring.
    url='{{addr}}/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds%7Bservice_name%3D%22'"$svc"'%22%7D&from=now-{{window}}&until=now&format=pprof'
    tmp="$(mktemp)"
    if ! curl -fsS --max-time 30 "$url" -o "$tmp" || [[ ! -s "$tmp" ]]; then
        rm -f "$tmp"
        echo "pgo-fetch: Pyroscope unreachable or returned no profile data ({{addr}})." >&2
        echo "pgo-fetch: keeping existing $dir/default.pgo (if any); builds fall back to non-PGO." >&2
        exit 1
    fi
    mv "$tmp" "$dir/default.pgo"
    echo "pgo-fetch: wrote $dir/default.pgo ($(wc -c < "$dir/default.pgo" | tr -d ' ') bytes) — picked up automatically by go build / goreleaser (-pgo=auto)"
