# go-boilerplate

A **production-grade, opinionated Go microservice boilerplate** for highload / enterprise teams. The repo is split into two zones: `platform/` is the reusable starter kit — config, structured logging, OTel observability, CQRS typed-decorator pipeline, Kafka outbox/inbox, two-tier cache, blob storage, resilience, auth/authz, audit, feature flags, and graceful lifecycle management — all with zero business logic. `examples/` contains four deletable demonstration services (gateway, orders, payments, notifications) that show how to wire the platform together; remove `examples/`, `proto/`, and `gen/` to get a clean blank canvas. The local stack runs via `task up` / `task up:full` (`docker compose` with profiles): Postgres, Redpanda (Kafka + Schema Registry), Redis, MinIO, Keycloak (core), plus OTel Collector, Jaeger, Prometheus, Grafana, Pyroscope (observability profile), and the four app services (apps profile).

---

## Stack

| Concern | Choice |
|---|---|
| Go version | 1.25 (container-aware `GOMAXPROCS`, no `automaxprocs`) |
| HTTP edge | `net/http` 1.22 routing + chi + oapi-codegen v2 |
| Kafka client | **franz-go** (pure-Go, EOS-capable, cooperative-sticky) |
| Event contracts | **protobuf + buf + Schema Registry** (typed, compat-enforced) |
| Database driver | **pgx v5 + pgxpool** (tuned) + PgBouncer (transaction mode) |
| Query codegen | **sqlc** (CopyFrom / SendBatch for throughput) |
| Migrations | **goose** (embedded SQL + Go) |
| Config | **caarlos0/env v11** (struct-tag, zero reflection overhead) |
| Logging | **`log/slog`** API + **zap** backend via `zapslog` |
| L1 cache | **otter v2** (W-TinyLFU, loading cache) |
| L2 cache | **rueidis** (RESP3 client-side caching) |
| Object storage | **minio-go v7** behind `ObjectStore` interface |
| Resilience | **failsafe-go** (Retry/CB/Timeout/Bulkhead/RateLimit) |
| Observability | **OpenTelemetry** → OTel Collector → Jaeger + Prometheus; **Pyroscope** continuous profiling |
| Auth | **Keycloak** OIDC + **lestrrat jwx/v2** JWKS validation |
| AuthZ | RBAC behavior (roles/perms from token claims) |
| Feature flags | **OpenFeature** Go SDK + swappable provider |
| Reliable publish | **transactional outbox** + polling relay (DB→Kafka) |
| Idempotent consume | **inbox table** dedup (`ProcessOnce`) |
| CQRS | **typed generic decorators** (`platform/cqrs`) |
| DI | **manual constructor wiring** + `run.Closer` reverse-order teardown |
| Testing | **testify + testcontainers-go** + `uber-go/goleak` |
| Dev tooling | **just** + golangci-lint + buf + sqlc + air |

---

## Architecture — event choreography

```mermaid
sequenceDiagram
    participant Client
    participant Gateway
    participant Orders
    participant Payments
    participant Notifications

    Client->>Gateway: POST /orders (REST)
    Gateway->>Gateway: persist projection row (status=created)
    Gateway->>Kafka: orders.commands (CreateOrderCommand)
    Kafka->>Orders: consume CreateOrderCommand (inbox dedup)
    Orders->>Orders: INSERT order, enqueue outbox
    Orders->>Kafka: orders.events (OrderCreated) via outbox relay
    Kafka->>Gateway: orders.events → update projection (status=created)
    Kafka->>Payments: orders.events (inbox dedup)
    Payments->>Payments: process payment, enqueue outbox
    Payments->>Kafka: payments.events (PaymentProcessed) via outbox relay
    Kafka->>Gateway: payments.events → update projection (status=paid)
    Kafka->>Notifications: payments.events → notify customer
```

All domain state transitions are driven by Kafka events. The gateway owns a **read-model** (projection) updated by consuming events; REST queries are served from this projection — no sync RPC between services.

---

## Quickstart

**Prerequisites:** Docker, Go 1.25+, [just](https://just.systems/).

### Compose profiles

The stack is split into three profiles so you only run what you need:

| Profile | Services started | Command |
|---|---|---|
| _(none — core)_ | postgres, redpanda, redpanda-console, redis, minio, minio-setup, keycloak | `just up` |
| `observability` | core + otel-collector, jaeger, prometheus, grafana, pyroscope | `just up-obs` |
| `apps` | core + gateway, orders, payments, notifications | `just up-apps` |
| both | Everything | `just up-full` |

```bash
# Start everything (core infra + observability + apps)
just up-full

# Or: start just core infra for local development (run services via go run)
just up

# Stop everything and remove volumes
just down
```

```bash
# Create an order (auth disabled by default)
curl -s -XPOST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c1","amount_cents":1500,"currency":"USD"}' | jq .

# Watch the status transition created → paid
ORDER_ID=<id from step 2>
curl -s localhost:8080/orders/$ORDER_ID | jq .status

# Observe
open http://localhost:16686   # Jaeger traces
open http://localhost:8090    # Redpanda Console (topics, messages)
open http://localhost:3000    # Grafana (admin/admin; Prometheus + Pyroscope datasources)
open http://localhost:9001    # MinIO Console (minioadmin/minioadmin)
```

### Running tests

Tests use testcontainers-go and spin up real containers. **Docker must be running.**

```bash
just test          # go test ./... (3–5 min; starts Postgres, Redpanda, Redis, MinIO)
```

### Auth — Keycloak

Auth is **disabled** by default (`GATEWAY_AUTH_DISABLED=true`). To enable:

```bash
# In docker-compose.yml gateway environment, change to:
GATEWAY_AUTH_DISABLED: "false"
GATEWAY_JWKS_URL: http://keycloak:8080/realms/app/protocol/openid-connect/certs
GATEWAY_JWKS_ISSUER: http://keycloak:8080/realms/app
GATEWAY_JWKS_AUDIENCE: gateway
```

Keycloak is published on **host port 8180** (maps to container port 8080) to avoid conflict with gateway. Admin console: `http://localhost:8180` (admin/admin). A pre-configured realm `app` with client `gateway` and roles `admin`/`user` is imported at startup.

```bash
# Fetch a demo token and call the API (gateway listens on host port 8080)
TOKEN=$(just token)
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/orders/<id>
```

---

## Project layout

```
go-boilerplate/
├── platform/        ★ THE BOILERPLATE — reusable, zero business logic
│   ├── config/      caarlos0/env loader
│   ├── log/         slog setup (zapslog backend), ctx logger, trace-id injection
│   ├── run/         lifecycle: signal handling, ordered start, Closer, two-phase shutdown
│   ├── telemetry/   OTel tracer + meter + log providers, OTLP exporters
│   ├── httpserver/  chi server, middleware (recover, req-id, OTel, slog, rate-limit, auth)
│   ├── httpx/       decode+validate helpers, RFC 7807 problem+JSON errors
│   ├── health/      /livez + /readyz aggregator
│   ├── pg/          pgxpool factory (tuned), tx runner, reader/writer split, health
│   ├── outbox/      outbox table + polling relay (DB → Kafka)
│   ├── kafka/       franz-go producer + consumer group, OTel, retry-topics, DLT
│   ├── serde/       protobuf ↔ Schema Registry serializer
│   ├── inbox/       idempotent-consumer (inbox pattern, ProcessOnce)
│   ├── outboxkafka/ wires outbox.Relay to the Kafka producer
│   ├── cqrs/        HandlerFunc + Behavior decorators (log/trace/metrics/validate/tx/cache)
│   ├── cache/       two-tier: otter v2 (L1) + rueidis (L2) + singleflight + TTL jitter
│   ├── blob/        ObjectStore interface + minio-go implementation
│   ├── resilience/  failsafe-go policy builders
│   ├── auth/        OIDC/JWT validation (Keycloak JWKS), pluggable middleware, ctx principal
│   ├── authz/       RBAC behavior + policy seam
│   ├── audit/       audit behavior → audit topic/table
│   └── featureflags/ OpenFeature wrapper + provider
│
├── examples/        ★ DELETABLE — demonstrates platform usage
│   ├── gateway/     REST edge; publishes commands; owns read-model projection
│   ├── orders/      consumes commands; emits OrderCreated via outbox
│   ├── payments/    consumes OrderCreated; emits PaymentProcessed via outbox
│   └── notifications/ consumes events; mock notify
│
├── proto/           event contracts (.proto files)
├── gen/             committed generated code (protobuf Go types)
├── deploy/          prometheus.yml, otel-collector.yaml, grafana provisioning,
│                    keycloak realm, postgres init SQL
├── docs/            ARCHITECTURE.md, ADRs, adding-a-service guide
├── Dockerfile       parametric multi-stage (--build-arg SERVICE=<svc>)
├── docker-compose.yml full local stack
├── justfile         dev loop: up/up-obs/up-apps/up-full/down/logs/test/lint/buf/sqlc/build-images
├── .air.toml        air hot-reload config (default target: skeleton; override via just dev <svc>)
└── buf.yaml         buf v2 config (proto lint + breaking rules)
```

**To start a new service from scratch:** delete `examples/`, `proto/`, `gen/proto/` — `platform/` never imports `examples/`, so the boundary is clean.

See [`docs/adding-a-service.md`](docs/adding-a-service.md) for a step-by-step guide.
