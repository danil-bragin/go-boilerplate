# Round 2 — Polish & Capabilities Plan

> **For agentic workers:** execute lane-by-lane; TDD for behavior changes; one commit per task;
> commit messages normal English + "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.
> Source findings: 4-agent post-remediation audit (2026-06-10, round 2). Decision: user approved ALL
> (ON-HOLD stays: cosign-images (needs registry), KIP-932 (needs brokers)).

Waves (merge between waves; lanes within a wave are file-disjoint):
- **Wave 1:** R1 docs-hygiene · R3 test-health · R4 observability-consumption
- **Wave 2:** R2 api-consistency · R5a light capabilities (k6, fuzz, _FILE, m2m, docs)
- **Wave 3:** R5b heavy capabilities (leader worker, SSE, chaos test)
- **Final:** full verify (lint 0, short, serial full suite, race spot), review pass, memory update.

## R1 — docs hygiene (Q1)
| id | task |
|---|---|
| R1.1 | rewrite `platform/README.md` from current code (jwx v3, aws-sdk/SeaweedFS, token-bucket, index tiers, +consume/+msgctx/+servicekit rows, cache: GetOrLoad/pubsub-inv/breaker; drop nonexistent resilience.RateLimit) |
| R1.2 | `.env.example`: add ALL ~30 missing envs w/ defaults + comments (servicekit block, gateway fix-round knobs, ORDERS_*, CACHE_*, PG_* timeouts, TELEMETRY_TRACE_RATIO, OTEL_METRICS_PROMETHEUS); keep claim "all vars" true |
| R1.3 | ARCHITECTURE.md: pipeline order (Tracing outermost), telemetry row (no logs provider — until R4.2 lands, then logs opt-in), GCRA→token bucket, ownership row += order GET/LIST, featureflags "env-var provider" → in-memory |
| R1.4 | small fixes: CONTRIBUTING Go 1.26+; justfile:98 + conventions.md:162 minio→seaweedfs; operations.md OTLP self-contradiction (keep the Note version); `dlq.go:36` stale comment; openapi: add 503 responses + fix lifecycle line (… → paid \| payment_failed \| payment_timeout); JWKS-outage semantics paragraph (ops doc, 401-by-design + ERROR log); config.Secret mention in conventions + .env.example note |
| R1.5 | archive `docs/superpowers/plans/2026-06-10-audit-remediation.md` → docs/history/superpowers/plans/ (header already says COMPLETE; add note "checkboxes unticked — execution tracked via commits"); keep THIS round-2 plan live until done |
| R1.6 | testkit: wire `fakes.NewVerifier` into gateway_test (replace local stubVerifier) OR remove from testing.md table — prefer wire; fixtures.Principal/Record: add one real usage in examples/testing or drop from docs table |

## R3 — test health (Q3)
| id | task |
|---|---|
| R3.1 | record serial constraint: justfile `test-integration` + `test` get `-p 1` (or GOTESTFLAGS env); ci.yml test job `-p 1` + timeout 30m + comment (4 vCPU + testcontainers); testing.md section "Docker capacity & parallelism" (exit-133 symptom documented) |
| R3.2 | shared containers: `platform/messaging/kafka` TestMain w/ one Redpanda (kafkatest gains `NewSharedRedpanda(m *testing.M)` or pkg-level lazy singleton + unique topic names per test — pick simplest reliable); same for `examples/gateway` (one pg+redpanda, unique DB/schema or table-prefix per test). Target: kafka pkg <80s, gateway <150s. NO test semantics change |
| R3.3 | fast lane: telemetry short tests — inject 100ms shutdown timeouts (test-only config), target package <5s, fast lane total <25s |
| R3.4 | goleak: add to servicekit lifecycle tests, kafka run/shutdown tests, outbox relay tests, httpserver server tests (with standard kgo/testcontainer ignores helper in testkit) |

## R4 — observability consumption (Q4)
| id | task |
|---|---|
| R4.1 | `deploy/grafana/provisioning/dashboards/` + 3 dashboards JSON: choreography (consumer lag, DLT rate, outbox pending+published, inbox dups), edge (RED by route, 429/503, ratelimit remaining, breaker state), runtime (GC, goroutines, heap vs GOMEMLIMIT); provisioning yaml; verified loadable (json valid, schemaVersion sane) |
| R4.2 | `deploy/prometheus/rules.yaml` (~8 alerts: lag growth 5m, outbox backlog age, dlt_produced>0 15m, commit_failures>0, breaker open >1m, readyz flapping, 5xx ratio, p99 latency); prometheus.yml rule_files+wiring; compose mount |
| R4.3 | OTel logs export opt-in: `TELEMETRY_LOGS=true` → otlploggrpc exporter + otelslog bridge handler fan-out (stdout stays); collector pipeline logs section; test: records reach in-memory exporter; docs operations.md §logging update + ARCHITECTURE telemetry row |

## R2 — API consistency (Q2)
| id | task |
|---|---|
| R2.1 | StandardPipeline adoption: orders/payments/notifications app handlers → `StandardPipeline(...).Use(audit...)`; move "inbox owns tx" comment into WithTransaction godoc; pipeline godoc: resilience stays transport-level; adding-a-service §6 + conventions: when Decorate vs StandardPipeline (answer: StandardPipeline default, raw Decorate = custom orders) |
| R2.2 | key-parking: `NewEscalator` honors `Policy.KeyParkingWindow` (option overrides); sizing guidance godoc+ops ("window ≥ tier0 + redelivery lag; DefaultPolicy → ~10s"); test: policy-field-only → parking active |
| R2.3 | header constants: `kafka.HeaderMessageID`/`kafka.HeaderEventType`; replace literals in outboxkafka, consume, redrive, fakes.Broker, gateway server.go |
| R2.4 | event-type/type safety: `consume.TypedFor[T proto.Message](version int, h func...)` deriving "`<proto full name>.v<N>`"; assert derived == current literals (orders.v1.OrderCreated → "orders.OrderCreated.v1"? CHECK actual literal convention — current is "orders.OrderCreated.v1" while proto full name is "orders.v1.OrderCreated"; derive correctly to KEEP wire format unchanged); migrate 4 services + producers (outboxkafka EventType source) to shared constants produced by one helper; quadruplication dies |
| R2.5 | export `kafka.RecordFromKGO` (+headers), delete retry's copy |
| R2.6 | redrive: use `kafka.NewClient(kafka.Config)`, `--brokers` defaults from `KAFKA_BROKERS`; `just redrive *ARGS` |
| R2.7 | retry knobs: servicekit Config `CONSUMER_RETRY_MAX_ATTEMPTS` (3) `CONSUMER_RETRY_BACKOFF` (100ms); `retry.Policy.FastBackoff` (default 100ms) used by WrapHandler |
| R2.8 | consume: `OnCommitted` → TypedOption (keep back-compat variadic until examples migrated, then drop — single repo: just change signature) |
| R2.9 | ratelimit: unify unknown sentinel (-1 for both Limit/Remaining), add `RateLimit-Reset` (delta-seconds) when known; update both impls+middleware+tests |
| R2.10 | topic envs: rule "named after topic, no service prefix" → gateway `GATEWAY_*_TOPIC` → `ORDERS_COMMANDS_TOPIC`/`ORDERS_EVENTS_TOPIC`/`PAYMENTS_EVENTS_TOPIC`; payments' misnamed `ORDERS_EVENTS_TOPIC` (its OUTPUT) stays (it IS orders.events? verify — payments publishes payments.events; if env misnames, fix to `PAYMENTS_EVENTS_TOPIC`); compose/.env/docs/e2e updated; conventions.md records the rule |
| R2.11 | msgctx ctx-keys → empty-struct style (match auth/log/pg); MaxBytes-less route group → startup WARN in httpserver |

## R5a — light capabilities
| id | task |
|---|---|
| R5a.1 | k6: `scripts/k6/order-flow.js` (POST w/ Idempotency-Key → poll to terminal; thresholds p99<500ms, err<1%, optional auth via TOKEN env); `just load [vus] [duration]`; ops doc §load-testing |
| R5a.2 | fuzz: FuzzClientIPKey (XFF walk), FuzzCursorDecode (list pagination), FuzzParseRetryHeaders, FuzzHTTPXDecode; `just fuzz` (30s each); CI job non-blocking 30s/target |
| R5a.3 | `_FILE` secrets: config.Load post-parse pass — for any `config.Secret` field w/ env tag X, if X empty && X_FILE set → read file (trim \n); tests; ops doc mapping (docker secrets / ESO / SOPS) |
| R5a.4 | m2m: realm-export service-account client (`gateway-m2m`, client-credentials, roles), `just token-m2m`, docs auth section; integration test optional (keycloak container heavy — reuse existing keycloak test if cheap) |
| R5a.5 | docs: flagd compose-profile snippet + example flags json (doc-only); GDPR/crypto-shredding page (patterns: per-subject key, delete=forget, retention vs compaction, projection deletion); ADR-0010 "opening the hatch" checklist (deadline/retry-budget/breaker) |

## R5b — heavy capabilities
| id | task |
|---|---|
| R5b.1 | leader worker: `pg.RunAsLeader(ctx, pool, name, fn)` (advisory-lock acquire/retry/health, extracted pattern from relay — relay keeps own impl or migrates if trivially); `servicekit.AddPeriodicWorker(name, interval, jitter, singleActive bool, fn)`; refit unpaid_watcher + inbox/audit cleaners onto it; tests: single-active across 2 instances, jitter bounds, teardown |
| R5b.2 | SSE: `GET /v1/orders/{id}/events` — projection upsert publishes status-change to redis channel `orders:status:<id>`; SSE handler streams (heartbeat 15s, Last-Event-ID resume from projection row, auth+ownership same as GET); openapi (text/event-stream); exempt from TimeoutHandler (per-group, pattern exists); integration test: POST → SSE receives created→paid sequence |
| R5b.3 | chaos: toxiproxy testcontainer test — kafka outage mid-flow: POST orders during broker partition → after heal: zero loss, zero duplicate side effects (inbox count), relay drains backlog; document in testing.md |

## Final gate
lint 0 · gofumpt · short lane (<25s target) · full serial suite green · spot -race (messaging+cache+servicekit) · e2e ×2 · compose+k8s valid · review pass over round-2 diff · memory update.
