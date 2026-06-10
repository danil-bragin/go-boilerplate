# platform/ — reusable boilerplate packages

Grouped by domain (≤2 levels). Package names are unqualified (import `go-boilerplate/platform/messaging/kafka`, use `kafka.X`).

`platform/` has zero business logic and never imports `examples/`. Delete `examples/`, `proto/`, and `gen/` to get a clean starter kit — the boundary is enforced.

---

## messaging/ — event flow

- **kafka** — franz-go (kgo) producer + consumer group; cooperative-sticky rebalancing; OTel instrumentation; `WithRetry` blocking in-process retry; `TransactConsumer` (EOS); `EnsureTopics`
- **consume** — standard typed consumer pipeline: event-type header dispatch (`Typed("orders.OrderCreated.v1", fn)`), serde or raw-proto decoding, uniform message-id policy, inbox idempotency, principal propagation; unknown event types are skipped, never errored
- **retry** — tiered retry-topic redrive: failed records go to index-named tier topics (`<base>.retry.0`, `.retry.1`, …) with a due-time header, then `<base>.DLT` after the final tier; optional best-effort key parking to limit per-key reordering
- **msgctx** — correlation-id + causation-id chain metadata in `ctx`; installed by `consume`, stamped onto outgoing messages by `outbox.Repository.Enqueue`
- **serde** — protobuf ↔ Confluent Schema Registry serializer (wire-format framing); schema registration + caching; raw-proto fallback when no registry is configured
- **outbox** — transactional outbox table + polling relay (`FOR UPDATE SKIP LOCKED`); AT-LEAST-ONCE delivery via injected `Publisher`; `WithSingleActive` advisory-lock leader; age-based `Cleaner`
- **inbox** — idempotent-consumer dedup (`ProcessOnce`): inserts inbox row + runs fn in the same DB transaction; periodic cleanup
- **outboxkafka** — wires `outbox.Relay` to the Kafka producer (keyed by aggregate id); optional Schema Registry framing via `WithEncoder`; bridges the two packages without a circular import

## observability/ — logs, traces, metrics, health

- **log** — `log/slog` API on a zap backend (`zapslog`); `From`/`Into` context helpers; trace_id + span_id auto-appended on `*Context` log calls when a span is active
- **telemetry** — OTel tracer + meter providers; OTLP/gRPC exporter; Prometheus-pull `/metrics` reader stays on even when OTLP export is disabled; W3C propagation; `Shutdown`
- **health** — `/livez` + `/readyz` aggregator; `Check` funcs; liveness reflects process health only, readiness gates on registered checks and flips to 503 on shutdown

## web/ — HTTP edge

- **httpserver** — chi server; middleware stack (SecurityHeaders, RequestID, OTel, AccessLog, Recover, MaxBytes, Timeout, RouteTag); CORS, `RateLimitPer`+`ClientIPKey` (per-key, trusted-proxy aware) and legacy global `RateLimit` opt-in; graceful `Shutdown`
- **httpx** — `Decode`+validate request bodies; RFC 7807 `problem+json` error responses
- **ratelimit** — `Limiter` interface; `NewMemory` (per-key in-process token bucket with idle eviction) and `NewRedis` (distributed atomic Lua token bucket over rueidis, fail-open by default); used by `RateLimitPer`

## security/ — authn/authz/audit

- **auth** — OIDC/JWT verification via lestrrat jwx/v3; JWKS auto-refresh; `Principal` in `ctx`; pluggable `Verifier` middleware; principal header inject/extract for message hops
- **authz** — RBAC as a CQRS behavior (`Require[C,R](policy, action)`); extracts roles from the `ctx` principal; returns `ErrUnauthenticated` / `ErrForbidden` for the edge to map to 401/403
- **audit** — CQRS behavior that records successful commands atomically inside the command's transaction (must sit INSIDE `Transaction` in the pipeline); audit table store + age-based cleanup

## storage/ — persistence + state

- **pg** — tuned `pgxpool` with reader/writer split; `RunInTx`; `FromContext`/`FromContextRead` (tx or pool); goose migration runner; health check
- **cache** — two-tier cache: L1 = otter v2 (in-process W-TinyLFU), L2 = rueidis (Redis RESP3); `GetOrLoad` with singleflight stampede protection; cross-instance L1 invalidation via Redis pub/sub (best-effort, TTL-bounded); L2 circuit breaker (`ErrL2Unavailable`); TTL jitter
- **blob** — `ObjectStore` interface + aws-sdk-go-v2 S3 implementation (SeaweedFS locally, AWS S3 in prod); `Put`/`Get`/`Delete`/`PresignGet`

---

## standalone

- **cqrs** — typed generic `HandlerFunc[C,R]` + `Behavior[C,R]` decorators; `Decorate` (outermost-first; Tracing MUST be outermost); built-in behaviors: Logging, Tracing, Metrics, Validation, Transaction, Caching, Deadline
- **resilience** — failsafe-go policy builders: `Retry`, `CircuitBreaker`, `Timeout`, `Bulkhead`; compose via `Do`/`Get[T]` execution helpers
- **config** — `caarlos0/env` struct-tag loader; `Load[T]()` returns a typed config value; `Secret` string type that redacts itself in logs/JSON; optional `.env` file loading
- **run** — signal handling (`SIGINT`/`SIGTERM`); reverse-order `Closer`; two-phase graceful shutdown
- **servicekit** — shared service harness: logger, telemetry, pg pool+migrations, kafka client+producer, health checks, admin HTTP (`/livez` `/readyz` `/metrics`), consumer wiring with retry/DLT, outbox relay+cleanup, public HTTP servers, readiness-first graceful teardown; `WithoutKafka`/`WithoutPG` options; `Main` entry point (automaxprocs + signal handling)
- **featureflags** — OpenFeature Go SDK wrapper; `BoolValue`/`StringValue` helpers; in-memory provider included (`NewInMemory`); swap in flagd/LaunchDarkly via `openfeature.SetProviderAndWait`; domain-scoped provider isolation
- **testkit** — test doubles: `fakes` (in-memory Broker, Cache, ObjectStore, Publisher, Verifier), `mockhttp` (recording server + JWKS/JWT mint), `mocks` (moq-generated), `fixtures` (builders for `auth.Principal`, `kafka.Record`, `outbox.Message`)
