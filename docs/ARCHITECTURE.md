# Architecture

## Platform packages

`platform/` is the reusable boilerplate layer. It has zero business logic and zero imports from `examples/`. Each package is independently usable.

| Package | Purpose |
|---|---|
| `config` | `caarlos0/env` struct-tag loader; `Load[T]()` returns a typed config value |
| `log` | `log/slog` setup; optional zap backend via `zapslog`; `FromContext`/`WithContext`; trace-id injection |
| `run` | Signal handling (`SIGINT`/`SIGTERM`), ordered `Start`, reverse-order `Closer`, two-phase shutdown |
| `telemetry` | OTel tracer + meter + logger providers; OTLP/gRPC exporter; `Shutdown` |
| `httpserver` | chi server; middleware stack (recover, req-id, OTel, slog, rate-limit, auth); graceful `Shutdown` |
| `httpx` | `Decode`+validate request bodies; RFC 7807 `ProblemJSON` error responses |
| `health` | `/livez` + `/readyz` aggregator; `Checker` interface; liveness always 200, readiness gates on registered checks |
| `pg` | `pgxpool` factory with tuned defaults; `RunInTx`; `FromContext` (pulls tx or pool); reader/writer pool split; health check |
| `outbox` | Outbox table (`outbox_messages`); `Repository.Enqueue`; polling `Relay` (`FOR UPDATE SKIP LOCKED`); AT-LEAST-ONCE delivery to Kafka |
| `kafka` | franz-go producer + consumer group; OTel instrumentation; cooperative-sticky; retry-topics (`<topic>.retry.N`) + DLT |
| `serde` | Protobuf ↔ Confluent Schema Registry serializer; schema registration + caching |
| `inbox` | `ProcessOnce(consumer, msgID, fn)` — inserts inbox row + runs `fn` in the SAME transaction; duplicate messages silently no-op |
| `outboxkafka` | Wires `outbox.Relay` to the Kafka producer; resolves the circular-import problem |
| `cqrs` | `HandlerFunc[C,R]`, `Behavior[C,R]`, `Decorate` — typed generic decorator pipeline |
| `cache` | Two-tier `Get`/`Set`/`Delete`: L1 = otter v2 (in-process), L2 = rueidis (Redis); singleflight stampede prevention; TTL jitter |
| `blob` | `ObjectStore` interface + minio-go v7 implementation; `Put`/`Get`/`Delete`/`PresignGet` |
| `resilience` | failsafe-go policy builders: `Retry`, `CircuitBreaker`, `Timeout`, `Bulkhead`, `RateLimit`; compose via `failsafe.With` |
| `auth` | OIDC/JWT validation via lestrrat jwx/v2; JWKS auto-refresh; `Principal` in `ctx`; pluggable middleware interface |
| `authz` | RBAC `Behavior` — extracts roles from `ctx` principal; returns 403 if required permission absent |
| `audit` | `Behavior` — on successful command writes an audit entry (who/what/when/resource) to the audit table via `pg.FromContext` |
| `featureflags` | OpenFeature Go SDK wrapper; `BoolValue`/`StringValue` helpers; swappable provider (env-var provider included) |

---

## CQRS + typed decorator behaviors

```go
type HandlerFunc[C, R any] func(context.Context, C) (R, error)
type Behavior[C, R any]    func(next HandlerFunc[C, R]) HandlerFunc[C, R]

func Decorate[C, R any](h HandlerFunc[C, R], behaviors ...Behavior[C, R]) HandlerFunc[C, R]
```

`Decorate` wraps `h` with behaviors in declaration order (first listed = outermost = runs first).

### Pipeline ordering

**Commands** (state-mutating, always transactional):

```
Logging → Tracing → Metrics → Validation → Audit → Transaction → handler
```

**Queries** (read-only, may be cached):

```
Logging → Tracing → Metrics → Validation → Caching → handler
```

**Invariants:**
- The `Transaction` behavior is applied ONLY to commands. It calls `pg.RunInTx` and stores the tx in `ctx` so that repositories and the `audit` behavior can call `pg.FromContext` without opening a new transaction.
- `inbox.ProcessOnce` already opens a transaction for Kafka consumers; command handlers wired to it omit the `Transaction` behavior to avoid a redundant savepoint.
- Repositories never begin transactions — they always pull from `ctx` via `pg.FromContext`.
- The `Caching` behavior is applied ONLY to queries; it short-circuits before any DB call on a hit.

---

## Effectively-once via outbox + inbox

The system achieves effectively-once processing over at-least-once Kafka delivery using two complementary patterns:

### Producer side — transactional outbox

1. The command handler inserts a domain event row into `outbox_messages` inside the same DB transaction as the state change.
2. The `Relay` polls for unpublished rows using `SELECT ... FOR UPDATE SKIP LOCKED`, publishes each to Kafka, and marks it published — all in a single transaction per batch.
3. If the Kafka `Publish` succeeds but the DB commit fails, the row is re-published later — delivering the message at least once. The relay sets a stable `message-id` header (the outbox row UUID) on every Kafka record.

### Consumer side — inbox dedup

1. The Kafka consumer calls `inbox.ProcessOnce(consumer, messageID, fn)`.
2. `ProcessOnce` inserts `(consumer, message_id)` into the `inbox` table with `ON CONFLICT DO NOTHING` and runs `fn` in the same transaction.
3. If the row already exists (duplicate delivery), `fn` is not called and the message is silently acknowledged.
4. If `fn` fails, the transaction rolls back — the inbox row is NOT persisted — so the message is reprocessed on the next delivery attempt.

**Net effect:** each business effect executes exactly once per message, even under Kafka rebalances and broker retries, with no Kafka EOS (Exactly-Once Semantics) overhead.

---

## Event flow (example domain)

```
Client → Gateway (REST POST /orders)
  └─► orders.commands  [CreateOrderCommand protobuf]
        └─► Orders service (inbox dedup → CreateOrder handler → outbox)
              └─► orders.events  [OrderCreated protobuf]
                    ├─► Gateway (projection: status = created → paid)
                    ├─► Payments service (inbox dedup → process payment → outbox)
                    │     └─► payments.events  [PaymentProcessed protobuf]
                    │           ├─► Gateway (projection: status = paid)
                    │           └─► Notifications service (inbox dedup → notify)
                    └─► (other future consumers)
```

Topics: `orders.commands`, `orders.events`, `payments.events`.

---

## DB topology

DB-per-service (separate logical databases in one Postgres instance locally). Each service owns its own migrations, tables, and connection pool. Cross-service reads go through Kafka events, not shared tables.

---

## Observability

- **Traces:** OTel SDK → OTLP/gRPC → OTel Collector → Jaeger UI (`localhost:16686`).
- **Metrics:** OTel SDK metrics → OTel Collector Prometheus exporter (`localhost:8889`); Prometheus scrapes the collector. Grafana visualises.
- **Logs:** `log/slog` API with trace-id injection; JSON format in production.
- **Profiling:** Pyroscope continuous profiling agent (`localhost:4040`); `pprof` endpoints behind auth guard.
- **Health:** `/livez` (always 200 if the process is alive); `/readyz` (gates on DB pool + Kafka connection).

> **Note:** Service-level `/metrics` Prometheus endpoints are a documented gap in v1. Prometheus currently scrapes the OTel Collector's own Prometheus exporter (`otel-collector:8889`) which re-exports all OTel-instrumented metrics forwarded from services. Direct `/metrics` handlers per service are a planned enhancement.

---

## Deferred / not-yet-built

The following items are documented design decisions deferred to a later iteration:

| Item | Notes |
|---|---|
| Per-service `/metrics` Prometheus endpoint | Services emit metrics via OTel SDK → collector; direct scrape endpoint not wired |
| Kafka EOS (`GroupTransactSession`) | Outbox+inbox is simpler and covers v1 requirements; EOS reserved for money-grade atomic consume→produce |
| Stateless retry-topics | Framework support exists in `platform/kafka` (DLT wiring); tiered retry topic routing not yet wired per service |
| Distributed rate limiting | `platform/resilience` has `redis_rate` GCRA integration; not wired into `httpserver` by default |
| Multi-tenancy | Tenant-id context + event propagation is a documented seam; not built in v1 |
| CDC outbox relay | Debezium-based relay is the scale-out path for high-throughput outbox; the polling relay is sufficient for v1 |
| Read replica pool routing | `pg.Pool` supports reader/writer split; replica routing not enabled in the example services |
