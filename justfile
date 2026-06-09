set shell := ["bash", "-uc"]

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

# Regenerate the gateway HTTP server from openapi.yaml (oapi-codegen)
oapi:
    cd examples/gateway && oapi-codegen --config oapi.gen.yaml openapi.yaml

# Regenerate moq mocks for platform interfaces (goimports normalizes moq's import grouping)
gen-mocks:
    go generate ./platform/testkit/mocks/...
    goimports -w -local go-boilerplate platform/testkit/mocks/

# Run tests for a package or pattern (default: ./...)
test pkg='./...':
    go test {{pkg}}

# Fast lane: unit + functional tests only (-short, no Docker)
test-unit:
    go test -short ./...

# Full lane: all tests including integration (requires Docker)
test-integration:
    go test ./...

# Run end-to-end tests only (requires full stack via docker compose)
test-e2e:
    go test ./examples/e2e/...

# Fast lane with coverage: outputs summary line to stdout
test-cover:
    go test -short -coverprofile=coverage.out ./...
    go tool cover -func=coverage.out | tail -1

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

# Start core infra (pg, kafka, redis, minio, keycloak)
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

# ── Utilities ─────────────────────────────────────────────────────────────────

# Apply a service's embedded migrations (PG_DSN / PG_MIGRATE_URL from env; svc: gateway|orders|payments|notifications|all)
migrate svc:
    go run ./cmd/migrate -service {{svc}}

# Fetch a Keycloak access token (demo/demo) for manual API calls — requires jq
# Usage: curl -H "Authorization: Bearer $(just token)" http://localhost:8080/v1/orders/<id>
token:
    curl -s -d client_id=gateway -d username=demo -d password=demo -d grant_type=password http://localhost:8180/realms/app/protocol/openid-connect/token | jq -r .access_token

# Install git hooks via lefthook
hooks:
    lefthook install

# Local snapshot release via goreleaser (requires goreleaser; snapshot = no publish)
release:
    goreleaser release --snapshot --clean
