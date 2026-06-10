# Architecture

> **File organisation, package boundaries, and tooling:** see [`docs/conventions.md`](conventions.md).

## Platform packages

`platform/` is the reusable boilerplate layer. It has zero business logic and zero imports from `examples/`. Each package is independently usable.

| Package | Import path (under `go-boilerplate/`) | Purpose |
|---|---|---|
| `config` | `platform/config` | `caarlos0/env` struct-tag loader; `Load[T]()` returns a typed config value |
| `log` | `platform/observability/log` | `log/slog` setup; optional zap backend via `zapslog`; `FromContext`/`WithContext`; trace-id injection |
| `run` | `platform/run` | Signal handling (`SIGINT`/`SIGTERM`), ordered `Start`, reverse-order `Closer`, two-phase shutdown |
| `telemetry` | `platform/observability/telemetry` | OTel tracer + meter + logger providers; OTLP/gRPC exporter; `Shutdown` |
| `httpserver` | `platform/web/httpserver` | chi server; middleware stack (SecurityHeaders, recover, req-id, OTel, access-log, max-bytes, timeout); CORS, `RateLimitPer`+`ClientIPKey`, and legacy `RateLimit` opt-in; graceful `Shutdown` |
| `ratelimit` | `platform/web/ratelimit` | `Limiter` interface; `NewMemory` (per-key in-process token bucket, janitor eviction) and `NewRedis` (atomic Lua GCRA, fail-open default) |
| `httpx` | `platform/web/httpx` | `Decode`+validate request bodies; RFC 7807 `ProblemJSON` error responses |
| `health` | `platform/observability/health` | `/livez` + `/readyz` aggregator; `Checker` interface; liveness always 200, readiness gates on registered checks |
| `pg` | `platform/storage/pg` | `pgxpool` factory with tuned defaults; `RunInTx`; `FromContext` (pulls tx or pool); reader/writer pool split; health check |
| `outbox` | `platform/messaging/outbox` | Outbox table; `Repository.Enqueue` (explicit `Topic`, auto correlation/causation stamping); polling `Relay` (`FOR UPDATE SKIP LOCKED`, drain-until-empty, single-active advisory-lock leader mode); AT-LEAST-ONCE delivery to Kafka |
| `kafka` | `platform/messaging/kafka` | franz-go producer + consumer group; OTel instrumentation; cooperative-sticky; seek-back redelivery on handler failure; `WithRetry` poison→DLT; `WithOnError` operational hook |
| `retry` | `platform/messaging/retry` | Tiered retry topics (`<topic>.retry.<idx>`, delay carried in `retry-due-at` header) + escalation to DLT; opt-in key parking to preserve per-key order |
| `consume` | `platform/messaging/consume` | `consume.Typed` — typed consumer pipeline: event-type header dispatch, serde/proto decode, uniform message-id policy, inbox dedup, principal + chain-lineage ctx install |
| `msgctx` | `platform/messaging/msgctx` | Correlation-id / causation-id context propagation (consume → outbox chain lineage) |
| `serde` | `platform/messaging/serde` | Protobuf ↔ Confluent Schema Registry wire format via franz-go `pkg/sr`; opt-in (`SERDE_SR_URL`); registration + caching |
| `inbox` | `platform/messaging/inbox` | `ProcessOnce(consumer, msgID, fn)` — inserts inbox row + runs `fn` in the SAME transaction; duplicate messages silently no-op |
| `outboxkafka` | `platform/messaging/outboxkafka` | Wires `outbox.Relay` to the Kafka producer; resolves the circular-import problem |
| `cqrs` | `platform/cqrs` | `HandlerFunc[C,R]`, `Behavior[C,R]`, `Decorate`, `StandardPipeline`, `Deadline` — typed generic decorator pipeline |
| `cache` | `platform/storage/cache` | Two-tier `Get`/`Set`/`Delete`/`GetOrLoad`: L1 = otter v2 (in-process), L2 = rueidis (Redis); cross-instance L1 invalidation via Redis pub/sub; circuit breaker on L2; singleflight stampede prevention; TTL jitter |
| `blob` | `platform/storage/blob` | `ObjectStore` interface + aws-sdk-go-v2 S3 implementation (SeaweedFS in local dev — ADR-0012); `Put`/`Get`/`Delete`/`PresignGet` |
| `resilience` | `platform/resilience` | failsafe-go policy builders: `Retry`, `CircuitBreaker`, `Timeout`, `Bulkhead`; compose via `failsafe.With` |
| `auth` | `platform/security/auth` | OIDC/JWT validation via lestrrat jwx/v3 (clock skew, optional azp); JWKS auto-refresh; `Principal` in `ctx`; Kafka header propagation (`InjectHeaders`/`ExtractToContext`) |
| `authz` | `platform/security/authz` | RBAC `Behavior` — extracts roles from `ctx` principal; returns 403 if required permission absent |
| `audit` | `platform/security/audit` | `Behavior` — on successful command writes an audit entry (who/what/when/resource) to the audit table via `pg.FromContext` |
| `featureflags` | `platform/featureflags` | OpenFeature Go SDK v2 wrapper; `BoolValue`/`StringValue` helpers; swappable provider (env-var provider included) |
| `servicekit` | `platform/servicekit` | Service wiring harness: `New` (+`WithoutKafka`/`WithoutPG`), `AddConsumer`/`AddConsumerWithRetry`/`AddOutboxRelay`/`AddWorker`/`AddHTTPServer`/`AddAuditCleanup`, `Main` entry point; readiness-first drain-gate teardown |

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

### Principal propagation over Kafka

The edge service that verified the caller's JWT injects `principal-sub` and `principal-roles` record headers (`auth.InjectHeaders`) when producing commands; the shared consumer pipeline (`platform/messaging/consume`) installs them back into the handler context (`auth.ExtractToContext`), so the audit behavior records the real actor instead of `anonymous`. These headers are **transport metadata, not authentication** — any client with produce rights can forge them. The trust boundary is the Kafka cluster itself: restrict produce access to command/event topics with broker ACLs and authenticate inter-service connections (mTLS/SASL). Never make authorization decisions from these headers for data that may originate outside that perimeter.

---

## Event flow (example domain)

```
Client → Gateway (REST POST /v1/orders)
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

Failure path: payments with `amount_cents >= 1_000_000` are declined (deterministic demo rule) → `PaymentFailed` on `payments.events` → projection status `payment_failed` + failure notification. Orders unpaid past `ORDERS_PAYMENT_DEADLINE` emit `OrderPaymentTimedOut` (orders service unpaid watcher, exactly once) → projection status `payment_timeout`. Projection terminal precedence: `pending < created < {paid, payment_failed, payment_timeout}` — first terminal wins, later terminal events are ignored with a WARN.

Chain lineage: every record carries `correlation-id` (constant per chain, == the root command's message id, seeded by the gateway) and `causation-id` (the direct parent's message id). `consume.Typed` installs both into ctx; `outbox.Enqueue` stamps them onto outgoing events automatically (`platform/messaging/msgctx`).

Projection split seam: the gateway runs the read-model projection embedded by default (`GATEWAY_EMBEDDED_PROJECTION=true`). Setting it to `false` and deploying `examples/gateway/cmd/projection` (consumer-only binary, admin server only) splits the edge from the read-model builder; both share the `gateway-projection` consumer group and inbox, so the handover is safe.

---

## DB topology

DB-per-service (separate logical databases in one Postgres instance locally). Each service owns its own migrations, tables, and connection pool. Cross-service reads go through Kafka events, not shared tables.

---

## Observability

- **Traces:** OTel SDK → OTLP/gRPC → OTel Collector → Jaeger UI (`localhost:16686`). Trace IDs are injected into structured log entries via the `traceHandler` in `platform/observability/log`, so every log line carries `trace_id` and `span_id` when a span is active.
- **Metrics:** OTel SDK MeterProvider wired in every service via `platform/observability/telemetry`. Two export paths:
  1. OTLP/gRPC → OTel Collector → Prometheus exporter (`otel-collector:8889`) — batch export.
  2. Direct Prometheus scrape endpoint `/metrics` on each service's admin HTTP server (`platform/observability/telemetry` registers a Prometheus exporter with the MeterProvider and mounts it on the admin mux).
- **Logs:** `log/slog` API with trace-id injection; JSON format in production. Trace correlation available in every request handler via `log.From(ctx)`.
- **Profiling:** Pyroscope continuous profiling agent (`localhost:4040`); `pprof` endpoints behind auth guard.
- **Health:** `/livez` and `/readyz` endpoints are mounted on every service's admin HTTP server. Liveness always returns 200 while the process is alive; readiness gates on registered checks (DB pool ping, Kafka client connectivity). The gateway additionally registers a Redis cache readiness check when cache is configured.

---

## Container runtime tuning

See `docs/operations.md` for the full rationale. Summary:

| Knob | Mechanism | Why |
|---|---|---|
| `GOMAXPROCS` | `go.uber.org/automaxprocs` blank-imported in `platform/servicekit/main.go` (`servicekit.Main`, the shared entry point of every service binary) | Reads the Linux cgroup CPU quota so the scheduler uses the correct thread count instead of the host CPU count, avoiding CFS throttling |
| `GOMEMLIMIT` | Set in `docker-compose.yml` (and `.env.example`) per service | Instructs the Go GC to soft-trim below the cgroup limit, preventing OOM-kill |
| CPU limit | `deploy.resources.limits.cpus` in `docker-compose.yml` | Bounds container CPU; use whole-number values to avoid fractional CFS throttling |
| Memory limit | `deploy.resources.limits.memory` in `docker-compose.yml` | Hard cgroup limit; `GOMEMLIMIT` should be ≈90% of this value |

---

## Wired in current version (was "deferred")

The following items were previously listed as deferred but are now implemented:

| Item | Status |
|---|---|
| Per-service `/metrics` Prometheus endpoint | Wired via `platform/observability/telemetry` — Prometheus exporter registered on MeterProvider; `/metrics` mounted on admin server for every service |
| Trace ID in logs | Wired via `platform/observability/log` traceHandler — `trace_id` + `span_id` injected into every log entry when a span is active |
| Health endpoints | `/livez` + `/readyz` mounted on every service's admin HTTP server via `platform/observability/health` |
| Security headers | `SecurityHeaders` middleware in default `httpserver.New` chain; sets `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Content-Security-Policy`, `X-XSS-Protection` |
| Edge rate limiting (global) | Legacy `RateLimit(rps, burst)` token-bucket middleware still available in `platform/web/httpserver`, but no longer wired anywhere — the gateway uses the per-IP limiter (next row); 429 responses carry `Retry-After`/`RateLimit-Remaining` headers + problem+json body |
| CORS | `CORS(CORSOptions)` middleware in `platform/web/httpserver`; wired on the gateway's public server; opt-in elsewhere |
| Resilience + caching + authz in gateway | `resilience.Do` (Retry ×3 + Timeout 2 s) wraps the Kafka command publish; CQRS caching behavior on GetOrder query; RBAC authz on CreateOrder; ownership check on attachments |
| CDC outbox relay (polling) | Polling relay (`platform/messaging/outbox`) with `FOR UPDATE SKIP LOCKED`, publish-after-commit, AT-LEAST-ONCE delivery wired in all services |
| Image signing (cosign) | Step present in `.github/workflows/ci.yml` (commented; requires registry credentials + `id-token: write`) |
| Per-IP + distributed rate limiting | `RateLimitPer(l, ClientIPKey(trusted))` in `platform/web/httpserver`; wired in gateway replacing the global limiter. `platform/web/ratelimit` provides `NewMemory` and `NewRedis`. Gateway config: `RATELIMIT_RPS`, `RATELIMIT_BURST`, `RATELIMIT_REDIS`, `TRUSTED_PROXIES`. |
| Kafka tiered retry-topics | `platform/messaging/retry` — index-named tiers (`<topic>.retry.<idx>`, delay in the `retry-due-at` header); used in the orders service |
| Kafka EOS (`TransactConsumer`) | `platform/messaging/kafka.TransactConsumer` — atomic consume→produce via `GroupTransactSession`; see ADR-0006 |

---

## Deferred / not-yet-built

The following items are genuine gaps deferred to a later iteration:

| Item | Notes |
|---|---|
| Multi-tenancy | Tenant-id context + event propagation is a documented seam; not built in v1 |
| TLS (inter-service) | All connections are plaintext (HTTP, OTLP `WithInsecure`, `sslmode=disable`, Kafka `PLAINTEXT`). In production TLS terminates at the ingress layer or service mesh. |
| Table partitioning / age-based cleanup | Age-based cleanup (polling delete of old published outbox rows, old audit rows) is wired; range-based Postgres table partitioning for true hot/cold archival is deferred. |
| Read replica pool routing | `pg.Pool` supports reader/writer split; replica routing not enabled in the example services |
| Image signing activated | `cosign sign` step in CI is commented out pending a container registry |
