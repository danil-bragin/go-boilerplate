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

# Render the public API docs into ./site (Redoc HTML + raw contracts) — same
# bundle the Docs workflow publishes. Open site/index.html in a browser.
docs:
    mkdir -p site/proto
    npx --yes @redocly/cli@1 build-docs examples/gateway/openapi.yaml --output site/index.html
    cp examples/gateway/openapi.yaml site/openapi.yaml
    cp docs/errors.md site/errors.md
    find proto -name '*.proto' -exec cp {} site/proto/ \;
    @echo "docs → site/index.html"

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
# Pinned to 2.57.0 to match the squawk binary CI downloads (.github/workflows/ci.yml).
lint-sql:
    npx -y squawk-cli@2.57.0 $(git ls-files '*/migrations/*.sql' '*/migrations/sql/*.sql')

# Validate Prometheus rules + config and run the rule unit tests via the
# dockerized promtool (same image the compose stack pins) — same gate as CI.
# The config check mounts the rule files at the exact paths prometheus.yml
# references (mirrors the compose volume layout).
promtool:
    docker run --rm -v "$PWD/deploy/prometheus:/r:ro" --entrypoint promtool prom/prometheus:v3.12.0 \
      check rules /r/rules.yaml /r/rules-latency.yaml /r/slo.yaml
    docker run --rm \
      -v "$PWD/deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
      -v "$PWD/deploy/prometheus/rules.yaml:/etc/prometheus/rules.yaml:ro" \
      -v "$PWD/deploy/prometheus/rules-latency.yaml:/etc/prometheus/rules-latency.yaml:ro" \
      -v "$PWD/deploy/prometheus/slo.yaml:/etc/prometheus/slo.yaml:ro" \
      --entrypoint promtool prom/prometheus:v3.12.0 \
      check config /etc/prometheus/prometheus.yml
    docker run --rm -v "$PWD/deploy/prometheus:/r:ro" --entrypoint promtool prom/prometheus:v3.12.0 \
      test rules /r/tests/slo_test.yaml

# Run golangci-lint with auto-fix
lint-fix:
    golangci-lint run --fix ./...

# Format code via golangci-lint fmt
fmt:
    golangci-lint fmt ./...

# Vulnerability scan (govulncheck) — version pinned to match CI (.github/workflows/ci.yml)
vuln:
    go run golang.org/x/vuln/cmd/govulncheck@v1.3.0 ./...

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

# Apps with Tier-1 scale levers ON (PgBouncer pooling, reader split, partitioned
# outbox, tuned relay, more Kafka partitions) — see docs/operations.md § Scaling.
up-scale:
    docker compose -f docker-compose.yml -f docker-compose.scale.yml --profile pgbouncer --profile apps up -d --build

# One-command proof of life: full stack up → create an order → watch it reach "paid" — requires jq.
# Idempotent: re-runs reuse the running stack and the same Idempotency-Key maps to the same order.
# DEMO_GATEWAY_URL / DEMO_KEYCLOAK_URL override the default host ports (e.g. when 8080 is taken).
demo:
    #!/usr/bin/env bash
    set -euo pipefail
    gw="${DEMO_GATEWAY_URL:-http://localhost:8080}"
    kc="${DEMO_KEYCLOAK_URL:-http://localhost:8180}"
    echo "▸ starting the stack (core + observability + apps — the first run builds 4 images, allow a few minutes)"
    docker compose --profile observability --profile apps up -d --build
    echo "▸ waiting for keycloak + gateway (up to 90s)"
    token=""; ready=""
    for _ in $(seq 1 90); do
        token="$(curl -s --max-time 2 -d client_id=gateway -d username=demo -d password=demo -d grant_type=password -d scope=openid \
            "$kc/realms/app/protocol/openid-connect/token" 2>/dev/null | jq -r '.access_token // empty' || true)"
        if [[ -n "$token" && "$(curl -s --max-time 2 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $token" "$gw/v1/orders?limit=1")" == "200" ]]; then
            ready=1; break
        fi
        sleep 1
    done
    [[ -n "$ready" ]] || { echo "demo: gateway not ready after 90s — inspect with 'just logs'" >&2; exit 1; }
    # The read path is ownership-checked (non-admin sees only customer_id == sub),
    # so the order must be created AS the demo user — sub via the userinfo endpoint.
    sub="$(curl -s --max-time 5 -H "Authorization: Bearer $token" "$kc/realms/app/protocol/openid-connect/userinfo" | jq -r '.sub // empty')"
    [[ -n "$sub" ]] || { echo "demo: could not resolve the demo user's subject" >&2; exit 1; }
    echo "▸ POST /v1/orders (Idempotency-Key: demo-order-0001 — retries/re-runs return the same order)"
    order_id="$(curl -s --max-time 5 -XPOST "$gw/v1/orders" \
        -H "Authorization: Bearer $token" \
        -H 'Content-Type: application/json' \
        -H 'Idempotency-Key: demo-order-0001' \
        -d '{"customer_id":"'"$sub"'","amount_cents":1500,"currency":"USD"}' | jq -r '.order_id // empty')"
    [[ -n "$order_id" ]] || { echo "demo: order creation failed — inspect with 'just logs'" >&2; exit 1; }
    echo "  order_id=$order_id"
    echo "▸ polling status until 'paid' (gateway → Kafka → orders → payments → read model; up to 60s)"
    status=""
    for _ in $(seq 1 60); do
        # problem+json carries a numeric .status — only trust .status on an order body
        status="$(curl -s --max-time 5 -H "Authorization: Bearer $token" "$gw/v1/orders/$order_id" | jq -r 'if .order_id then .status else empty end')"
        [[ "$status" == "paid" ]] && break
        sleep 1
    done
    [[ "$status" == "paid" ]] || { echo "demo: order stuck at '${status:-unknown}' after 60s — inspect with 'just logs'" >&2; exit 1; }
    echo "▸ final order:"
    curl -s --max-time 5 -H "Authorization: Bearer $token" "$gw/v1/orders/$order_id" | jq .
    echo ""
    echo "Demo complete — the order traveled gateway → Kafka → orders → payments → back into the gateway read model."
    echo ""
    echo "Explore the running stack:"
    echo "  Gateway API       $gw            (token: just token)"
    echo "  Jaeger traces     http://localhost:16686"
    echo "  Grafana           http://localhost:3000   (admin/admin)"
    echo "  Redpanda Console  http://localhost:8090"
    echo ""
    echo "The stack stays up. Stop everything with: just down"

# Stop everything and remove volumes
down:
    docker compose --profile observability --profile apps --profile pgbouncer down -v

# Stream logs from all services
logs:
    docker compose --profile observability --profile apps --profile pgbouncer logs -f

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
      grafana/k6:2.0.0@sha256:a33a0cfdc4d2483d6b7a3a22e726a499ff2831a671a49239104cd34a9937523c \
      run --vus {{vus}} --duration {{duration}} - < scripts/k6/order-flow.js

# Correctness-under-load: seeded adversarial traffic against a running gateway (see docs/operations.md §Load testing)
# Usage: just traffic [--rate 50 --duration 1m | --phases "10rps:5s,40rps:20s"] [--seed N] [--mix happy=70,sse=0] [--token "$TOKEN"]
traffic *ARGS:
    go run ./cmd/trafficgen {{ARGS}}

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
