# platform/ — reusable boilerplate packages

Grouped by domain (≤2 levels). Package names are unqualified (import `go-boilerplate/platform/messaging/kafka`, use `kafka.X`).

`platform/` has zero business logic and never imports `examples/`. Delete `examples/`, `proto/`, and `gen/` to get a clean starter kit — the boundary is enforced.

---

## messaging/ — event flow

- **kafka** — franz-go producer + consumer group; OTel instrumentation; cooperative-sticky rebalancing; retry-topics (`<topic>.retry.N`) + DLT
- **serde** — protobuf ↔ Confluent Schema Registry serializer; schema registration + caching
- **outbox** — transactional outbox table + polling relay (`FOR UPDATE SKIP LOCKED`); AT-LEAST-ONCE delivery to Kafka
- **inbox** — idempotent-consumer dedup (`ProcessOnce`): inserts inbox row + runs fn in the same DB transaction
- **outboxkafka** — wires `outbox.Relay` to the Kafka producer; bridges the two packages without a circular import

## observability/ — logs, traces, metrics, health

- **log** — `log/slog` setup; optional zap backend via `zapslog`; `FromContext`/`WithContext`; trace-id + span-id injection
- **telemetry** — OTel tracer + meter + logger providers; OTLP/gRPC exporter; `/metrics` Prometheus endpoint; `Shutdown`
- **health** — `/livez` + `/readyz` aggregator; `Checker` interface; liveness always 200, readiness gates on registered checks

## web/ — HTTP edge

- **httpserver** — chi server; middleware stack (SecurityHeaders, recover, req-id, OTel, access-log, max-bytes, timeout); CORS and RateLimit opt-in; graceful `Shutdown`
- **httpx** — `Decode`+validate request bodies; RFC 7807 `ProblemJSON` error responses

## security/ — authn/authz/audit

- **auth** — OIDC/JWT validation via lestrrat jwx/v2; JWKS auto-refresh; `Principal` in `ctx`; pluggable middleware interface
- **authz** — RBAC `Behavior` — extracts roles from `ctx` principal; returns 403 if required permission absent
- **audit** — `Behavior` — on successful command writes an audit entry (who/what/when/resource) to the audit table via `pg.FromContext`; includes age-based cleanup

## storage/ — persistence + state

- **pg** — `pgxpool` factory with tuned defaults; `RunInTx`; `FromContext` (pulls tx or pool); reader/writer pool split; goose migration runner; health check
- **cache** — two-tier `Get`/`Set`/`Delete`: L1 = otter v2 (in-process W-TinyLFU), L2 = rueidis (Redis RESP3); singleflight stampede prevention; TTL jitter
- **blob** — `ObjectStore` interface + minio-go v7 implementation; `Put`/`Get`/`Delete`/`PresignGet`

---

## standalone

- **cqrs** — typed generic `HandlerFunc[C,R]` + `Behavior[C,R]` decorators; `Decorate`; built-in behaviors: Logging, Tracing, Metrics, Validation, Transaction, Caching
- **resilience** — failsafe-go policy builders: `Retry`, `CircuitBreaker`, `Timeout`, `Bulkhead`, `RateLimit`; compose via `failsafe.With`
- **config** — `caarlos0/env` struct-tag loader; `Load[T]()` returns a typed config value
- **run** — signal handling (`SIGINT`/`SIGTERM`); ordered `Start`; reverse-order `Closer`; two-phase shutdown
- **featureflags** — OpenFeature Go SDK wrapper; `BoolValue`/`StringValue` helpers; swappable provider (env-var provider included)
- **testkit** — test doubles: `fakes` (in-memory Publisher, Cache, ObjectStore), `mockhttp` (recording server + JWKS), `mocks` (moq-generated), `fixtures` (test data builders)
