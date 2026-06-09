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

# Run all code generators (buf + sqlc + mocks)
gen:
    buf generate
    sqlc generate -f examples/gateway/internal/store/sqlc.yaml
    sqlc generate -f examples/orders/internal/store/sqlc.yaml
    sqlc generate -f examples/payments/internal/store/sqlc.yaml
    sqlc generate -f platform/messaging/outbox/sqlc.yaml
    go generate ./platform/testkit/mocks/...

# Regenerate moq mocks for platform interfaces
gen-mocks:
    go generate ./platform/testkit/mocks/...

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

# Fetch a Keycloak access token (demo/demo) for manual API calls — requires jq
# Usage: curl -H "Authorization: Bearer $(just token)" http://localhost:8080/orders/<id>
token:
    curl -s -d client_id=gateway -d username=demo -d password=demo -d grant_type=password http://localhost:8180/realms/app/protocol/openid-connect/token | jq -r .access_token

# Install git hooks via lefthook
hooks:
    lefthook install

# Local snapshot release via goreleaser (requires goreleaser; snapshot = no publish)
release:
    goreleaser release --snapshot --clean
