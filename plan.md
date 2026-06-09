# Go Microservice Boilerplate — Design Plan

Status: **draft / brainstorming**. Date: 2026-06-08.
Target: **highload enterprise** event-driven microservice platform.
Go **1.25+**. Picks below resolved against 4 research sweeps (layout/stack, CQRS-libs, DI, highload).

---

## 1. Goals & principles

- Production-grade, opinionated-but-not-a-framework Go microservice boilerplate for **highload / enterprise**.
- **Platform (boilerplate) cleanly separated from examples.** Delete `examples/` + `proto/` → pure starter kit.
- CQRS: command side owns transactions; query side read-only. Cross-cutting concerns as **pipeline behaviors** (typed decorators).
- Event-driven: services talk via **Kafka**. REST only at the edge (gateway).
- Idiomatic, **AI-friendly** Go: explicit > magic, compile-time safe, small consumer-side interfaces, popular well-documented libs.
- Highload-ready: resilience, idempotency, two-tier caching, pool tuning, graceful degradation, profiling.
- Everything runnable locally via `docker compose up` + `task`.

---

## 2. Stack — resolved

Legend: **LOCKED** = decided · **REC** = research-recommended, confirm · **OPT** = optional/escape-hatch documented.

| Concern | Pick | Status | Notes |
|---|---|---|---|
| Go version | **1.25+** | LOCKED | container-aware GOMAXPROCS → no automaxprocs |
| Layout | monorepo, single `go.mod`, `cmd/`+`internal/`, no `pkg/` | LOCKED | group-by-feature |
| Platform/example split | `platform/` vs `examples/` + import-lint | LOCKED | core requirement |
| Edge HTTP | `net/http` (1.22 routing) + **chi** | LOCKED | good enough for enterprise highload; fasthttp only for edge-proxy tier |
| Edge contract | **OpenAPI spec-first + oapi-codegen v2** | LOCKED | generates chi/net-http server + types + Swagger UI |
| Read path | **CQRS projections** (gateway owns read-model DB from events) | LOCKED | eventual consistency; larger systems may split a read service |
| Auth (edge) | **Keycloak (OIDC) + pluggable auth middleware** | LOCKED | RS256/JWKS validation, ctx principal; middleware iface = swap IdP |
| AuthZ | **RBAC behavior** (roles/perms from token claims) | LOCKED | `platform/security/authz` seam, applied per command/query |
| Audit | **audit behavior** (who-did-what on commands) → audit topic/table | LOCKED | |
| Feature flags | **OpenFeature** Go SDK + simple provider | LOCKED | runtime toggles; example provider, swap to real |
| Multi-tenancy | tenant-id ctx/event propagation | DEFERRED | documented seam, not built in v1 |
| Inter-service | **Kafka** | LOCKED | event-driven |
| Kafka client | **franz-go** | LOCKED | pure-Go (no CGO), EOS-capable, cooperative-sticky default, KIP-848 |
| Event contract | **protobuf + buf + Schema Registry** | LOCKED | typed events, compat enforcement |
| Internal sync RPC (if needed) | **connect-go** | OPT | stdlib net/http handler, gRPC-wire-compatible; only if read path needs sync |
| Reliable publish | **transactional outbox + polling relay** | LOCKED | Debezium CDC = documented scale-out path |
| Idempotent consume | **inbox table** (dedup by business key) | LOCKED | + at-least-once + idempotent producer |
| Delivery semantics | **at-least-once** default; **EOS** (`GroupTransactSession`) for atomic flows | LOCKED | EOS reserved for money-grade |
| CQRS impl | **typed generic decorators** (`platform/cqrs`) | LOCKED | drop go-mediatr |
| DI | **manual constructor wiring + reverse-order Closer helper** | LOCKED | samber/do v2 = documented upgrade path |
| DB driver/pool | **pgx v5 + pgxpool (tuned)** | LOCKED | max_conns ≈ cores×2..4; lifetime/idle reaping; MinConns>0 |
| Query codegen | **sqlc** (+ `:batch`/`:copyfrom`) | LOCKED | jet/hand-pgx only for dynamic queries |
| Bulk/throughput | **CopyFrom** (bulk) + **SendBatch** (N+1/upsert) | LOCKED | doc the CopyFrom no-RETURNING/ON-CONFLICT caveat |
| Conn pooler | **PgBouncer ≥1.21** transaction mode | LOCKED | doc prepared-stmt gotcha → `max_prepared_statements` or pgx describe-mode |
| Read scaling | reader/writer pgxpool split | LOCKED | replica-lag/read-your-writes caveats documented |
| Migrations | **goose** | LOCKED | embeddable, SQL+Go |
| Config | **cleanenv** | LOCKED | struct-tag, AI-predictable |
| Validation | **go-playground/validator** | LOCKED | 2025 default |
| Logging API | **`log/slog`** | LOCKED | stdlib API everywhere |
| Logging backend (highload) | **zap via `zapslog` handler** | LOCKED | slog API + zap speed; wrapped so backend is swappable |
| L2 cache | **rueidis** (RESP3 client-side caching) | LOCKED | wrapped behind cache iface to swap |
| L1 cache | **otter v2** (W-TinyLFU, loading) | LOCKED | ristretto DORMANT — avoid |
| Stampede | **singleflight** + Redis `SET NX` + **TTL jitter** | LOCKED | |
| Object storage | own `ObjectStore` iface + **minio-go v7** | LOCKED | MinIO local / S3 prod |
| Resilience | **failsafe-go** (unified) + `x/time/rate` + `redis_rate` + `x/sync/semaphore` | LOCKED | resilience4j analog; avoid dormant uber-go/ratelimit |
| Observability | **OpenTelemetry + Prometheus** | LOCKED | traces+metrics+logs |
| Profiling | **pprof** + **Pyroscope** (continuous) | LOCKED | |
| GC/runtime | **GOMEMLIMIT≈90% RAM**; profile before GOGC | LOCKED | no hardcoded GOMAXPROCS |
| Shutdown | **two-phase** (fail readiness → preStop drain → Shutdown) | LOCKED | `/livez` vs `/readyz` split |
| Testing | **testify + testcontainers-go** + `uber-go/goleak` | LOCKED | real pg/kafka in tests |
| Dev/build | **Taskfile + air + golangci-lint** | LOCKED | |
| Local infra | **docker-compose** | LOCKED | kafka, schema-registry, postgres, pgbouncer, redis, minio, keycloak, jaeger, prometheus, grafana, otel-collector, pyroscope |

---

## 3. Repo layout

```
go-boilerplate/
├── go.mod  Taskfile.yml  buf.yaml  buf.gen.yaml  docker-compose.yml  .env.example  openapi.yaml
│
├── platform/            ★ THE BOILERPLATE — zero business logic, reusable
│   ├── messaging/
│   │   ├── kafka/       franz-go producer + consumer-group, otel, cooperative-sticky, retry-topics + DLT
│   │   ├── serde/       protobuf <-> schema-registry serializer
│   │   ├── outbox/      outbox table + polling relay (DB→Kafka)
│   │   ├── inbox/       idempotent-consumer dedup (ProcessOnce)
│   │   └── outboxkafka/ bridges outbox.Relay to the Kafka producer
│   ├── observability/
│   │   ├── log/         slog setup (+ optional zapslog backend), ctx logger, trace-id
│   │   ├── telemetry/   OTel tracer+meter+log providers, OTLP exporters, shutdown
│   │   └── health/      /livez + /readyz aggregator
│   ├── web/
│   │   ├── httpserver/  chi server, middleware (recover, reqid, otel, slog, ratelimit, auth), graceful stop
│   │   └── httpx/       decode+validate, RFC7807 problem+json errors
│   ├── security/
│   │   ├── auth/        OIDC/JWT validation (Keycloak JWKS), pluggable middleware, ctx principal
│   │   ├── authz/       RBAC behavior + policy seam (roles/perms from claims)
│   │   └── audit/       audit behavior → audit topic/table
│   ├── storage/
│   │   ├── pg/          pgxpool factory (tuned), tx runner, reader/writer split, health
│   │   ├── cache/       two-tier: otter v2 (L1) + rueidis (L2) + singleflight + jitter
│   │   └── blob/        ObjectStore interface + minio-go impl
│   ├── config/          cleanenv loader, typed
│   ├── run/             lifecycle: signals, ordered start, reverse-order Closer, two-phase shutdown
│   ├── cqrs/            Handler[C,R] + Behavior decorators (log/trace/metrics/validate/tx/cache)
│   ├── resilience/      failsafe-go policy builders (retry/CB/timeout/bulkhead/ratelimit)
│   ├── featureflags/    OpenFeature wrapper + provider
│   └── testkit/         test doubles: fakes, mockhttp, mocks, fixtures
│
├── proto/               ★ event contracts (+ committed gen/)
│   └── <domain>/v1/*.proto
│
├── examples/            ★ DELETABLE — demonstrates platform usage
│   ├── servicekit/      shared consumer service harness (package servicekit)
│   ├── gateway/         REST edge (oapi-codegen); Keycloak auth; publishes commands;
│   │                    owns read-model DB (projections from events) → serves queries
│   ├── orders/          owns orders DB; commands (tx) emit OrderCreated via outbox
│   ├── payments/        consumes OrderCreated → emits PaymentProcessed
│   └── notifications/   consumes events → (mock) notify
│
├── deploy/              prometheus.yml, grafana dashboards, otel-collector, pyroscope config
└── docs/                ARCHITECTURE.md, ADRs, "how to add a service" guide
```

Per-service internal (pragmatic layered + CQRS):
```
examples/orders/
├── cmd/orders/main.go       manual wiring + Closer registration
├── internal/
│   ├── transport/           http handlers (gen) + kafka consumers
│   ├── app/
│   │   ├── commands/         command handlers (tx-decorated)
│   │   └── queries/          query handlers → query service → repo/cache
│   ├── domain/              entities + domain errors (no deps)
│   ├── store/               sqlc queries + pgx; reads tx from ctx, never opens it
│   └── events/              proto<->domain mapping, outbox publish
└── migrations/             goose
```

---

## 4. CQRS + behaviors (LOCKED)

Typed generic decorators (no go-mediatr):
```go
type HandlerFunc[C, R any] func(context.Context, C) (R, error)
type Behavior[C, R any]    func(next HandlerFunc[C, R]) HandlerFunc[C, R]
func Decorate[C, R any](h HandlerFunc[C, R], bs ...Behavior[C, R]) HandlerFunc[C, R] { /* reverse-wrap */ }
```
Pipeline: Logging → Tracing → Metrics → Validation → [Caching: queries] → [Transaction: commands] → Handler.

**Invariant:** tx boundary lives ONLY in `Transaction` behavior, applied ONLY to commands (marker). Queries read-only. Repos pull tx from `ctx`; never begin one.

---

## 5. DI (LOCKED) — manual + Closer

- Constructor DI wired in `main.go`. Compile-time safe, AI-predictable, zero dependency risk.
- `platform/run.Closer`: register `func(ctx) error` teardowns; run reverse-order on SIGTERM via errgroup + timeout.
- DI = boot-time only → **zero steady-state runtime cost** regardless of approach.
- Upgrade path if graph > ~25-30 deps: **samber/do v2** (built-in reverse `Shutdown()`+`HealthCheck()`, generics; accept runtime-resolve + single-maintainer).

---

## 6. Highload findings (research 2026-06-08)

**Resilience:** failsafe-go = unified resilience4j analog (Retry/CB/Timeout/Bulkhead/RateLimit/Fallback/Hedge/Adaptive-limiter). Compose `Fallback → Retry → CircuitBreaker → Timeout` (timeout innermost = per-attempt bound). In-proc limit `x/time/rate`; distributed `redis_rate` (GCRA). Bulkhead `x/sync/semaphore`; adaptive load-shed via failsafe adaptive limiter. **Always full jitter.** Avoid dormant: uber-go/ratelimit, slok/goresilience, mercari/go-circuitbreaker.

**Kafka:** at-least-once + idempotent producer + **outbox(producer)/inbox(consumer)** = effectively-once, simpler+faster than EOS. EOS (`GroupTransactSession`, auto-aborts on rebalance) only for atomic consume→produce. Key by aggregate (per-partition ordering only); over-provision partitions. cooperative-sticky / KIP-848. Non-blocking **retry-topics** (tiered delays) + `<topic>.DLT` with failure metadata; retry state must be stateless (survives rebalance). `zstd` compression, `linger.ms`+batch tuning, batch offset commits, `acks=all`+`min.insync.replicas=2`.

**Data:** pgxpool default max_conns=4 → tune. PgBouncer txn-mode breaks session prepared statements → PgBouncer ≥1.21 `max_prepared_statements` OR pgx `QueryExecModeDescribeExec`. CopyFrom ~6× SendBatch for bulk (but no RETURNING/ON CONFLICT) → CopyFrom fast-path, SendBatch fallback. sqlc confirmed best-in-class (lowest abstraction cost, pgx batch+copy, AI-friendly).

**Caching:** two-tier — L1 **otter v2** (ristretto dormant), L2 **rueidis** RESP3 client-side caching (~14× go-redis on hits). singleflight + Redis SET NX + TTL jitter for stampede.

**Transport/serde:** net/http (1.22) sufficient. encoding/json default; sonic on trusted hot paths (no full UTF-8 validation); json/v2 experimental in 1.25. connect-go for internal sync RPC (stdlib transport). Avoid json-iterator (superseded), goccy/go-json (crash reports).

**Logging:** zerolog/zap ~25-51ns vs slog ~101ns. Use slog API + **zapslog** backend at highload; sampling+async writer at extreme rates. Avoid zerolog-slog bridge (slow).

**Runtime/ops:** Go 1.25 container-aware → **drop automaxprocs**, don't hardcode GOMAXPROCS, set CPU *limits*. `GOMEMLIMIT≈90%` RAM prevents OOMKill. Two-phase shutdown: flip `/readyz`→503, preStop sleep ~5-15s for LB propagation, then drain (http Shutdown / grpc GracefulStop), close kafka (commit+leave), DB pools last. `terminationGracePeriodSeconds` > preStop+drain. Pyroscope continuous profiling + guarded `/debug/pprof`.

---

## 7. Architectural decisions (resolved)

- **O1 Read path:** ✅ CQRS projections — gateway owns read-model DB, populated by consuming domain events; serves queries from it. Eventual consistency.
- **O2 DB topology:** ✅ DB-per-service (separate DB per service in one Postgres container locally).
- **O3 Command flow:** gateway validates REST → publishes command event to Kafka → owning service consumes, executes command (tx), emits domain event via outbox. Gateway projection consumes domain events.
- **O4 Auth:** ✅ Keycloak (OIDC) in compose + pluggable auth middleware (`platform/security/auth`, RS256/JWKS). IdP swappable via interface.
- **O10 Enterprise:** ✅ RBAC + Audit + Feature flags (OpenFeature) in v1. Multi-tenancy = documented seam, deferred.
- **O11 Watermill evaluated → rejected:** Sarama-based (SyncProducer-only publish, per-message ack) vs our franz-go async-batch pipeline; router/outbox/CQRS batteries already built natively. Core stays 100% Kafka/franz-go.

Minor (sensible defaults, adjust if desired):
- **O5 Migrations:** `task migrate` job (goose), not on-start; on-start version-check guard only.
- **O6 CI:** GitHub Actions — golangci-lint, buf lint+breaking, `go test` w/ testcontainers, build images.
- **O7 Versioning:** monorepo single version tag; per-service container images tagged `<service>-<sha>`.
- **O8 Config:** per-service env prefix (e.g. `ORDERS_`, `GATEWAY_`); shared platform vars unprefixed.
- **O9 Bootstrap:** local module path `go-boilerplate` (no remote yet); MIT license; `git init` when ready.

---

## 8. Next steps
1. ✅ Stack + architecture locked.
2. Confirm module path (O9) + any tweaks.
3. Finalize → write spec → writing-plans → build platform, then examples.
