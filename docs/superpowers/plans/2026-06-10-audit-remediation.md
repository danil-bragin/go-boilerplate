# Audit Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every fix MUST ship with tests (TDD: failing test first). Caveman encoding used below per repo FORMAT.md conventions; code blocks verbatim.

**Goal:** Fix ALL findings from 2026-06-10 six-agent deep audit (messaging correctness, cache coherence, edge/security, observability, contracts, choreography failure paths, DX, stack updates).

**Architecture:** 6 phases. Phase 0 sequential (toolchain + critical kafka redelivery fix — everything builds on it). Phases 1–3 run parallel lanes in git worktrees (disjoint file sets), merged in stated order. Phase 4 big path moves (servicekit promotion) done AFTER feature lanes to avoid rebase hell. Phase 5 docs + final verification.

**Tech Stack:** existing locked stack + changes decided with user: keep otter L1 + add Redis pub/sub invalidation; wire Schema Registry via franz-go pkg/sr; MinIO → aws-sdk-go-v2 + SeaweedFS NOW; retry ordering = document + opt-in key-parking; Go 1.26; jwx v3; OpenFeature v2 path.

**User decisions (binding):**
- D1: cache = otter L1 KEPT + Redis pub/sub invalidation broadcast (not CSC-only).
- D2: serde = real SR wire format via `franz-go/pkg/sr`; protoprint approach deleted.
- D3: MinIO migration NOW: client → aws-sdk-go-v2 S3, local/dev → SeaweedFS, tests → generic testcontainer.
- D4: retry ordering = loud docs + opt-in key-parking mode.
- D5: scope = ALL findings. Every fix covered by tests.

---

## Traceability (finding → task)

| Finding | Task | | Finding | Task |
|---|---|---|---|---|
| C1 franz-go redelivery loss | 0.2 | | M7 messaging metrics | 1.6 |
| C2a readiness order | 2.1 | | M7 route RED/trace_id | 2.12–2.14 |
| C2b shutdown commit drop | 0.3 | | M8 error leaks | 2.4 |
| C3 audit anonymous/spoof | 2.2 | | M8 jwt skew/azp | 2.5 |
| C4 failure path | 3.1–3.3 | | M8 CORS | 2.6 |
| C4 replay/rebuild | 3.4–3.5 | | M9 migrations | 3.8 |
| C5 stale docs/servicekit zone | 4.1, 5.1 | | M10 202/404 UX | 2.11 |
| M1a relay throughput | 1.1 | | M11 scaffolding/rename | 4.4 |
| M1b multi-relay ordering | 1.2 | | M11 servicekit rigid | 4.2 |
| M2 SR serde dead | 2.8 | | M11 fast-lane tests | 4.6 |
| M2 buf non-blocking | 2.10 | | M11 admin bind | 3.9 |
| M3 partitions=1/RF=1 | 1.3 | | stack Go 1.26 | 0.1 |
| M4 cache (all) | 1.7–1.10 | | stack jwx v3 | 2.5 |
| M5 retry ordering | 1.4 | | stack OpenFeature | 1.12 |
| M6 idempotency-key | 2.3 | | stack MinIO | 1.11 |
| transact race #11 | 1.5 | | stack PGO | 5.4 |
| cleanup indexes #8 | 1.13 | | commit/fetch hooks #9 | 0.3 |
| inbox/retention #10 | 1.3, 3.4 | | contract drift #12 | 2.9 |
| typed consumer #13 | 2.9 | | retry topic names #14 | 1.4 |
| relay transport #15 | ADR amend 5.2 | | cqrs trace_id #5 | 2.13 |
| pg pool/timeouts #8,#9 | 3.7 | | cqrs Deadline/behaviors #10 | 3.6 |
| resilience dead #11 | 1.9, 3.6 | | config Validate/Secret #12 | 3.7 |
| closer #13 | 3.7 | | generics tax #14 | 3.6 |
| key versioning #15 | 1.10 | | timeout buffering W11 | 2.7 |
| 429 headers W13 | 2.7 | | authz resource W14 | 2.5 |
| edge gaps W15 | 2.14 | | OpenAPI fiction W9 | 2.11 |
| arch ADRs missing | 5.2 | | gateway split seam A5 | 3.5 |
| edge-produce undoc A9 | 5.2 | | platform versioning A10 | 5.2 |
| analytics story A11 | 5.2 | | DR/backup A8 | 5.3 |
| docs contradictions DX9 | 5.1 | | plan.md/superpowers DX10,11 | 5.1 |
| gen story DX12 | 4.4 | | mains dup DX13 | 4.3 |
| fakes.Broker DX14 | 4.5 | | arch tests DX15 | 4.7 |
| runbooks DX8 | 5.3 | | K8s/deploy A7 | 5.3 |

---

## Phase 0 — toolchain + critical kafka fix (SEQUENTIAL, inline)

### Task 0.1: Go 1.26 bump
**Files:** Modify `go.mod`, `Dockerfile`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`
- [ ] `go.mod`: `go 1.26`; `go get go@1.26 toolchain@go1.26.x && go mod tidy`
- [ ] Dockerfile builder image → `golang:1.26`; CI setup-go `go-version: "1.26.x"`
- [ ] Run: `go build ./... && go vet ./...` → clean
- [ ] Run: `just test-unit` → green
- [ ] Commit: `build: bump to Go 1.26 (Green Tea GC default)`

### Task 0.2: C1 — redelivery semantics fix (THE critical one)
**Files:** Modify `platform/messaging/kafka/run.go`, Test `platform/messaging/kafka/redelivery_test.go` (new, integration)

Background: kgo advances consume position on `PollFetches` regardless of commits. Current `Run` skips failed records and later `CommitRecords` commits PAST them → permanent loss on every "don't commit → redeliver" path (handler error, DLT-produce failure, escalate failure).

- [ ] **Step 1: failing integration test** (Redpanda testcontainer via existing `kafkatest` helper). Test cases:
```go
// TestRedeliveryOnHandlerFailure: produce r1,r2,r3 same partition.
// Handler fails r2 twice (transient), succeeds 3rd attempt.
// Assert: all 3 processed, r2 processed before r3 (order kept), no loss, no skip.
// TestNoCommitPastFailure: handler fails r2 permanently (always err).
// Stop consumer after 5 attempts, assert committed offset == r1's offset+1 (not r3's).
// TestFailureIsolatedPerPartition: r2(p0) fails, records on p1 keep flowing (non-blocking other partitions).
```
- [ ] **Step 2: run** `go test ./platform/messaging/kafka/ -run TestRedelivery -v` → FAIL (r2 lost, commit past)
- [ ] **Step 3: fix `Run`** — per-partition failure → seek-back loop:
```go
// in per-partition worker, on handler error at record rec:
//  1. stop processing rest of this partition's batch
//  2. commit nothing at/после rec for this partition (lastGood logic already per-partition — verify)
//  3. seek back: c.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
//        rec.Topic: {rec.Partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset}}})
//  4. backoff before next poll redelivers (exponential per topic/partition: 100ms→5s cap,
//     reset on success) — prevents hot-loop on poison record (WithRetry/DLT is the real answer,
//     backoff is the floor)
// CommitRecords: only ≤ lastGood per partition (audit existing logic, fix if global).
```
- [ ] **Step 4: run tests** → PASS; run existing kafka tests + `dlq_test.go` (WithRetry DLT-produce-failure path now genuinely redelivers — update test expectations if they encoded the broken semantics)
- [ ] **Step 5:** audit `retry/consumer.go:329,387` escalate-failure paths — same seek-back fix where "no commit" assumed redelivery. Add test: escalate produce fails → record redelivered, not lost.
- [ ] **Step 6:** fix doc comments in `run.go`, `dlq.go`, `docs/ARCHITECTURE.md` claiming "uncommitted ⇒ redelivered".
- [ ] Run: `go test ./platform/messaging/... -race` → green
- [ ] Commit: `fix(kafka): seek-back redelivery on handler failure — uncommitted records were silently lost`

### Task 0.3: C2b + #9 — shutdown commit + error hooks
**Files:** Modify `platform/messaging/kafka/run.go`, `examples/servicekit/consumers.go`, Test `platform/messaging/kafka/run_test.go`
- [ ] Failing test: cancel ctx mid-batch → final commit succeeds (fresh 5s ctx), commit error counted/logged not swallowed.
- [ ] Fix: `run.go:121` → on `ctx.Err()`, final commit with `context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)`; log+metric on commit error. Add `WithOnError(func(ctx, stage string, err error))` option to base consumer (mirror retry.Consumer); wire fetch-error (`run.go:69`) + commit-error through it. servicekit passes logger-backed hook.
- [ ] servicekit: ensure `Run` returns before kgo client close (check `lifecycle.go` ordering; fix wait if missing).
- [ ] Tests pass, race clean. Commit: `fix(kafka): fresh-ctx final commit on shutdown, surface fetch/commit errors via WithOnError`

---

## Phase 1 — parallel lanes (worktrees): L1 messaging, L2 cache, L3 storage/flags

Merge order: L2 → L3 → L1 (L1 largest diff, merges last onto fresh main).

### Lane L1 — messaging hardening

### Task 1.1: relay drain loop
**Files:** Modify `platform/messaging/outbox/relay.go`, Test `platform/messaging/outbox/relay_test.go`
- [ ] Failing test: insert 350 rows, BatchSize=100, one tick → all 350 published (drain), not 100.
- [ ] Fix `Run`: after tick, `for { n := ProcessBatch(); if n < BatchSize || ctx.Err() != nil { break } }`.
- [ ] Test latency: partial batch → sleeps (no busy loop). Commit: `fix(outbox): drain relay until empty per tick — was capped at BatchSize/interval`

### Task 1.2: single-active relay (ordering)
**Files:** Modify `platform/messaging/outbox/relay.go`, `examples/servicekit/relay.go`, docs in relay godoc, Test `relay_test.go`
- [ ] Failing test (pg testcontainer): 2 relays + `WithSingleActive` → only one publishes (other idle); kill leader's conn → standby takes over ≤ 2×interval; per-aggregate order preserved across failover.
- [ ] Impl: `WithSingleActive(pool *pgxpool.Pool)` — dedicated `pool.Acquire` conn, `SELECT pg_try_advisory_lock(hashtext('outbox_relay:'||schema))`; not acquired → poll-retry; conn health-checked each tick, lost → release + re-acquire loop. servicekit enables by default (`OUTBOX_SINGLE_ACTIVE=true` default).
- [ ] Fix relay godoc lie ("multiple instances safe and beneficial" → ordering caveat + leader mode).
- [ ] Commit: `fix(outbox): single-active relay via advisory lock — multi-relay broke per-aggregate ordering`

### Task 1.3: EnsureTopics spec + retention
**Files:** Modify `platform/messaging/kafka/admin.go`, `examples/servicekit/consumers.go`, `examples/servicekit/config.go`, Test `admin_test.go`
- [ ] Failing test: EnsureTopics with `TopicSpec{Partitions:6, RF:1, Configs:{"retention.ms":"604800000"}}` → topic created with exact partitions+config (verify via kadm).
- [ ] Impl: `type TopicSpec struct{ Partitions int32; ReplicationFactor int16; Configs map[string]string }`; `EnsureTopics(ctx, spec, topics...)`. servicekit config: `TOPIC_PARTITIONS` (default 6), `TOPIC_RF` (default 1 local; doc: prod ≥3), `TOPIC_RETENTION` (default 168h, set explicitly — inbox window invariant documented: inbox retention ≥ topic retention guard: startup WARN if violated), `ENSURE_TOPICS` (default true; doc: prod = IaC, set false).
- [ ] Commit: `feat(kafka): configurable partitions/RF/retention in EnsureTopics; servicekit defaults 6/1/7d`

### Task 1.4: retry ordering — docs + opt-in key-parking + index-based tier names
**Files:** Modify `platform/messaging/retry/retry.go`, `escalate.go`, `consumer.go`, `examples/servicekit/consumers.go`, Test `retry/parking_test.go`
- [ ] Tier names: `<base>.retry.<idx>` (0,1,2…) instead of `5m0s` duration strings; delay carried in `due-at` header only → policy tuning no longer strands in-flight records. Migration note in godoc (old-name topics drain via old consumers or manual redrive). Failing test first: TierTopic naming + consumer subscribes idx topics.
- [ ] Key-parking opt-in: `escalate.WithKeyParking(window time.Duration)`. Escalator keeps `map[parkKey]time.Time` (topic+key → parked-until = now+window), guarded mutex, lazily pruned. Consumer wrapper (servicekit fast-attempts wrap): before handler, if key parked → divert straight to lowest tier (preserve attempt header). Best-effort (in-memory, lost on rebalance) — documented.
- [ ] Tests: K1 fails→escalates→K1' next record diverted (handler never sees it), K2 unaffected, after window K1 flows normally; rebalance loses parking (doc'd, no test needed beyond unit prune test).
- [ ] LOUD package doc: tiered retry breaks per-key ordering unless parking enabled; gateway-projection-style consumers must be reorder-safe or use parking.
- [ ] Commit: `feat(retry): index-based tier topics + opt-in key-parking to preserve per-key order during escalation`

### Task 1.5: TransactConsumer race + doc
**Files:** Modify `platform/messaging/kafka/transact.go`, Test `transact_test.go`
- [ ] Failing race test (`-race` exposes): concurrent produce promises during commit decision.
- [ ] Fix: drop unguarded `len(produceErrs)` pre-check at `transact.go:197`; rely on `sess.End` abort-on-error semantics (kgo fails txn internally on produce error); keep produceErrs under mutex only for post-End error reporting. Doc serial-ProcessFn limitation in godoc.
- [ ] Commit: `fix(kafka): remove racy produce-error pre-check in TransactConsumer; document serial processing`

### Task 1.6: messaging metrics (lag, backlog, DLT)
**Files:** Create `platform/messaging/kafka/metrics.go`, Modify `relay.go`, `dlq.go`, `run.go`, Test `metrics_test.go`
- [ ] Metrics (otel meter, prometheus-exported): `outbox.pending` gauge (relay polls `SELECT count(*) ... WHERE published_at IS NULL` every 15s, cheap partial-index scan post-1.13), `outbox.published`/`outbox.publish_errors` counters; `kafka.consumer.records_processed`/`records_failed`/`commit_failures` counters {topic}; `kafka.dlt.produced` counter {topic}; `kafka.consumer.lag` gauge {topic,partition} via `HookFetchBatchRead` (HighWatermark − last consumed offset).
- [ ] Test: in-memory metric reader asserts counters move on process/fail/DLT; lag gauge set after fetch.
- [ ] Commit: `feat(messaging): lag/backlog/DLT/commit-failure metrics — primary failure modes now visible`

### Lane L2 — cache coherence (D1: otter + pub/sub)

### Task 1.7: Delete API + pub/sub invalidation broadcast
**Files:** Modify `platform/storage/cache/cache.go`, `platform/cqrs/caching.go`, Create `platform/storage/cache/invalidation.go`, Test `cache_test.go`
- [ ] Failing test (redis testcontainer): two `Cache` instances A,B share redis. A.Set(k); B.Get(k) warm L1. A.Delete(k) → B L1 evicted (poll-assert ≤500ms), B.Get → miss/reload. Also: A.Set(k, v2) → B sees v2 ≤500ms (invalidate-on-set broadcast).
- [ ] Impl: add `Delete(ctx, key) error` to `cache.Cache` + `cqrs.Cache` ifaces. Redis channel `cache:inv:<prefix>`: Delete → `DEL` + `PUBLISH key`; Set → `SET` + `PUBLISH key` (others drop L1 only). Each instance: dedicated rueidis subscribe (instance ID in payload, skip self for Set). Goroutine lifecycle via Closer; goleak test.
- [ ] Commit: `feat(cache): Delete + cross-instance L1 invalidation via redis pub/sub`

### Task 1.8: GetOrLoad in CQRS pipeline + singleflight ctx fix
**Files:** Modify `platform/cqrs/caching.go`, `platform/storage/cache/cache.go`, gateway `get_order.go`, Test `caching_test.go`, `cache_test.go`
- [ ] Failing tests: (a) 50 concurrent misses through `cqrs.Caching` → loader called once (singleflight reachable); (b) first caller ctx cancelled → other collapsed waiters still get value (loader runs on `context.WithoutCancel`).
- [ ] Impl: `cqrs.Cache` gains `GetOrLoad(ctx, key string, ttl time.Duration, load func(ctx) ([]byte, error)) ([]byte, error)`; `Caching` behavior uses it; `cache.Cache.GetOrLoad` loader ctx = `context.WithoutCancel(firstCtx)` + own timeout.
- [ ] Gateway projection write path calls `cache.Delete("gw:v1:order:"+id)` on upsert (closes write-stale loop).
- [ ] Commit: `fix(cache): stampede protection reachable from cqrs pipeline; detached loader ctx; projection busts cache`

### Task 1.9: L2 breaker (redis-down latency cliff)
**Files:** Modify `platform/storage/cache/cache.go`, Test `cache_test.go`
- [ ] Failing test: stop redis container → 100 Gets complete < 50ms each (L1/miss path, no per-op dial wait), breaker-open metric/log once; restart redis → recovers ≤30s.
- [ ] Impl: failsafe-go CircuitBreaker around all L2 ops (finally a real consumer for `resilience.CircuitBreaker`): 5 failures → open 15s, half-open probe. Open → L1-only. Metric `cache.l2.breaker_state`.
- [ ] Commit: `feat(cache): circuit breaker on L2 — redis outage no longer a per-request latency cliff`

### Task 1.10: key versioning convention
**Files:** Modify gateway `get_order.go`, `platform/storage/cache/cache.go` godoc, `docs/conventions.md`
- [ ] Gateway keys → `gw:v1:order:<id>`. Convention doc: `svc:vN:` prefix, bump N on result-shape change. Test: key constant updated.
- [ ] Commit: `docs(cache): versioned key convention; gateway adopts gw:v1: prefix`

### Lane L3 — storage/flags swaps

### Task 1.11: MinIO → aws-sdk-go-v2 + SeaweedFS (D3)
**Files:** Rewrite `platform/storage/blob/*.go`, Modify `docker-compose.yml`, `deploy/`, gateway attachments tests, `.env.example`, Test `blob_test.go`
- [ ] Failing tests: existing `ObjectStore` contract tests (Put/Get/Stat/Exists-404/Remove/Presign) repointed at SeaweedFS generic testcontainer (`chrislusf/seaweedfs`, cmd `server -s3`, wait for s3 port). Run → FAIL (no impl).
- [ ] Impl: `blob.New` on `aws-sdk-go-v2/service/s3` (+ `s3.PresignClient`): path-style option for local, region default, anonymous→static creds from config. Same `ObjectStore` iface — zero caller changes. Drop minio-go + its testcontainers module from go.mod.
- [ ] compose: minio+minio-setup → seaweedfs single container + bucket init (awscli one-shot or s3 API call in app — prefer init container with `amazon/aws-cli`). Update envs (`S3_ENDPOINT` etc. — names keep).
- [ ] ADR-0012: MinIO OSS maintenance-mode exit (evidence links), aws-sdk client + SeaweedFS dev choice.
- [ ] Commit: `feat(blob): aws-sdk-go-v2 S3 client + SeaweedFS local — MinIO OSS archived (ADR-0012)`

### Task 1.12: OpenFeature v2 import path
**Files:** Modify `platform/featureflags/*.go`, go.mod, Test existing flags_test
- [ ] Swap `github.com/open-feature/go-sdk/...` → `go.openfeature.dev/openfeature/v2`; adapt API diffs; tests green unchanged in behavior.
- [ ] Commit: `chore(featureflags): migrate to canonical go.openfeature.dev/openfeature/v2 path`

### Task 1.13: outbox/inbox cleanup indexes
**Files:** Create `platform/messaging/outbox/migrations/00002_cleanup_indexes.sql`, `platform/messaging/inbox/migrations/00002_processed_at_idx.sql`, Test cleanup tests assert index used (EXPLAIN)
- [ ] Migrations: `CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;` + `CREATE INDEX inbox_processed_at_idx ON inbox (processed_at);` (goose, `-- +goose NO TRANSACTION` + CONCURRENTLY variant documented in ops doc; plain CREATE INDEX fine for boilerplate default).
- [ ] Test: EXPLAIN on DeletePublishedBefore/inbox cleanup → index scan (assert plan contains index name).
- [ ] Commit: `perf(messaging): indexes for retention cleanup — were full-table scans hourly`

---

## Phase 2 — parallel lanes: L4 edge/security, L5 contracts/API, L6 observability

Merge order: L5 → L4 → L6 (L6 smallest, rebases easiest; L4/L6 both touch `httpserver/middleware.go` — L6 owns AccessLog/OTel funcs, L4 owns Timeout/CORS/ratelimit funcs in `security.go`; conflicts minimal, resolve at merge).

### Lane L4 — edge/security

### Task 2.1: C2a — readiness flips FIRST on shutdown
**Files:** Modify `examples/servicekit/service.go`, `lifecycle.go`, gateway `gateway.go`, Test `servicekit/lifecycle_test.go`
- [ ] Failing test: record teardown order via instrumented closers → sequence MUST be: drain-gate (SetNotReady + grace sleep) → public http shutdown → consumers → … → admin server LAST (so /readyz serves 503 during whole drain).
- [ ] Impl: servicekit gains `FinishWiring()` (or auto in `Run`): registers `closer.Add("drain-gate", ...)` LAST (LIFO → runs first): `health.SetNotReady(); select{ time.After(cfg.DrainGrace) / ctx }` (`DRAIN_GRACE` default 5s, 0 in tests). Admin server closer re-registered FIRST (runs last). Gateway: no change beyond servicekit (verify its http-server closer order).
- [ ] Commit: `fix(servicekit): readiness flips + grace period BEFORE any server shutdown; admin server dies last`

### Task 2.2: C3 — principal propagation Kafka
**Files:** Create `platform/security/auth/propagate.go`, Modify gateway `server.go`, servicekit/consume path, orders/payments transports, Test `propagate_test.go` + audit integration test
- [ ] Failing tests: (a) unit header round-trip `Principal{Sub, Roles}` → record headers `principal-sub`/`principal-roles` → ctx; (b) integration: POST with token → orders audit row `actor == sub` (not "anonymous").
- [ ] Impl: `auth.InjectHeaders(ctx, headers) []kgo.RecordHeader` + `auth.ExtractToContext(ctx, headers)`. Gateway producer injects. Consumer transport middleware extracts before pipeline. SECURITY NOTE (normal English in code/docs): headers are transport metadata, NOT authentication — trust boundary = broker ACLs/mTLS; documented in ARCHITECTURE.md security section.
- [ ] Commit: `feat(security): propagate principal via kafka headers — audit trail records real actor`

### Task 2.3: M6 — Idempotency-Key
**Files:** Modify gateway `server.go`, `openapi.yaml`, Test gateway integration test
- [ ] Failing test: two POST /v1/orders, same `Idempotency-Key` → same order id, ONE order row downstream (inbox dedups deterministic command id); no header → fresh uuid each.
- [ ] Impl: id = `uuid.NewSHA1(idempotencyNS, []byte(key))` (UUIDv5, deterministic); command message-id = order id → inbox drops dup. OpenAPI: header param documented.
- [ ] Commit: `feat(gateway): Idempotency-Key → deterministic order id; client retries no longer duplicate orders`

### Task 2.4: M8a — stop leaking err.Error()
**Files:** Modify gateway `routes.go`, `platform/security/auth/middleware.go`, Test gateway handler tests
- [ ] Failing tests: 500 path body == generic problem+json (no internal string); 400 → problem+json validation message (safe); auth verifier non-token error → generic 401 problem, detail logged not echoed.
- [ ] Impl: `ResponseErrorHandlerFunc`/`RequestErrorHandlerFunc` → `httpx.WriteProblem` + `log.From(ctx).ErrorContext` with request_id; auth middleware same.
- [ ] Commit: `fix(web): RFC7807 everywhere at strict-handler seam; internal errors logged, never echoed`

### Task 2.5: M8b + jwx v3 + authz resource
**Files:** Modify `platform/security/auth/*` (jwx v3 migration), `platform/security/authz/authz.go`, attachments handler, go.mod, Tests in auth/authz
- [ ] jwx v2→v3: imports, API diffs (`jwt.Parse` option names, jwk cache API). Existing tests green.
- [ ] Failing tests: (a) token iat=now+15s accepted with skew 30s, rejected with skew 0; (b) `WithAuthorizedParty("gateway")` → azp mismatch 401; (c) `Authorize(ctx, p, action, resource)` — attachments: principal ≠ order owner → 403, owner → 200.
- [ ] Impl: `AUTH_CLOCK_SKEW` (default 30s) → `jwt.WithAcceptableSkew`; optional `AUTH_REQUIRED_AZP`. Authz: `Policy.Authorize(ctx, p, action string, resource any) error`; RBAC impl ignores resource (compat); attachments passes order view, ownership check `view.CustomerID == p.Sub` (closes TODO(ownership)). Revocation: documented bound = token TTL (ops doc).
- [ ] Commit: `feat(security): jwx v3, clock skew + azp options, resource-aware authz + attachment ownership`

### Task 2.6: M8c — CORS hardening
**Files:** Modify `platform/web/httpserver/security.go`, gateway `config.go`, Test `security_test.go`
- [ ] Failing tests: default origins = deny-all (no ACAO header); `Vary: Origin` always present when Origin sent; disallowed preflight → no CORS headers (403 semantics).
- [ ] gateway `GATEWAY_CORS_ORIGINS` default `""` (was `*`). Commit: `fix(web): CORS deny-by-default + Vary: Origin`

### Task 2.7: 429 ergonomics + per-group timeout/maxbytes
**Files:** Modify `platform/web/ratelimit/ratelimit.go`, memory/redis impls, `platform/web/httpserver/security.go`, `middleware.go`, gateway `routes.go`, Tests across
- [ ] Failing tests: 429 response has real `Retry-After` + `RateLimit-Remaining` headers + problem+json body (both limiter impls return Result); attachment download streams (response > maxbytes limit of JSON routes) and not buffered by TimeoutHandler; attachment upload > 1MiB allowed up to `ATTACH_MAX_BYTES`.
- [ ] Impl: `Limiter.Allow(ctx, key) (ratelimit.Result, error)`, `Result{Allowed bool; Remaining int64; RetryAfter time.Duration}` (lua already computes; memory bucket exposes). Middleware emits headers + problem body. httpserver: `Timeout`/`MaxBytes` become per-route-group options; gateway mounts attachments group exempt from TimeoutHandler (context deadline instead) + higher MaxBytes. TimeoutHandler keeps for JSON routes with problem+json body (503 status documented as known TimeoutHandler limitation).
- [ ] Commit: `feat(web): rate-limit result headers + problem 429; per-group timeout/body limits; streaming routes unbuffered`

### Lane L5 — contracts + API honesty

### Task 2.8: D2 — real SR serde via franz-go pkg/sr
**Files:** Rewrite `platform/messaging/serde/` (delete protoprint code), Modify `platform/messaging/outboxkafka/publisher.go`, Test `serde_test.go` (Redpanda SR testcontainer)
- [ ] Failing tests: encode `OrderCreated` → bytes have Confluent wire format (magic 0, schema id, msg index); schema registered under subject `orders.events-value`; decode round-trips; second encode reuses cached id (no re-register); foreign/unknown schema id → typed error.
- [ ] Impl: `serde.New(srURL)` using `franz-go/pkg/sr` client + `sr.Serde`. Schema source: `go:embed` proto files from `proto/` (WKT imports fine on Redpanda SR — proven earlier). `Register[T proto.Message](s, subject, protoFile, msgIndex)`. outboxkafka encodes via serde (config opt: `SERDE_SR_URL`; unset → raw protobuf, keeps boilerplate runnable without SR — documented). Registration at startup idempotent, ctx honored (no `context.Background()`), failure → startup error (fail-fast) when SR enabled.
- [ ] Commit: `feat(serde): Confluent wire-format protobuf serde via franz-go pkg/sr; protoprint approach removed`

### Task 2.9: typed consumer + contract cleanup
**Files:** Create `platform/messaging/consume/consume.go`, Modify 4 services' transports + projection, `platform/messaging/outbox/outbox.go` (+migration), `outboxkafka/publisher.go`, Delete `proto/events/v1/*`, Test `consume_test.go`
- [ ] Failing test: `consume.Typed[*ordersv1.OrderCreated](serde, "orders.OrderCreated.v1", handler)` → kgo.Record in, typed proto out, inbox.ProcessOnce wrap, message-id policy uniform (header `message-id`, fallback topic+partition+offset), unknown event-type → skip+metric (no error).
- [ ] Refactor orders/payments/notifications transports + gateway projection onto `consume.Typed` (kills ~40 line/service copy-paste, unifies dedup semantics). Versioned event types: headers `event-type: orders.OrderCreated.v1`.
- [ ] `outbox.Message` gains explicit `Topic` field; migration `00003_topic.sql` adds column (backfill from aggregate_type); `topicFor` uses Topic; AggregateType back to real aggregate type.
- [ ] Delete dead `proto/events/v1` (envelope + duplicate OrderCreated); regen.
- [ ] Commit: `feat(messaging): typed consumer middleware, explicit outbox topic, versioned event types; drop dead events/v1`

### Task 2.10: CI contract gates blocking
**Files:** Modify `.github/workflows/ci.yml`
- [ ] `buf breaking` `continue-on-error` → removed (blocking); trivy `exit-code: "1"`; cosign activation checklist → CONTRIBUTING.md.
- [ ] Verify: CI config valid (`gh workflow` lint or yaml parse). Commit: `ci: buf breaking + trivy now blocking`

### Task 2.11: M10/W9 — API honesty (202, pending row, OpenAPI)
**Files:** Modify gateway `server.go`, `projection` store, `openapi.yaml`, regen `internal/api/gen.go`, README diagram, Test gateway integration
- [ ] Failing test: POST /v1/orders → 202 + `Location: /v1/orders/{id}`; immediate GET → 200 `{status:"pending"}` (NOT 404); after choreography → `paid`.
- [ ] Impl: CreateOrder pre-inserts projection row `status=pending` (upsert-safe vs racing OrderCreated event — event upgrade pending→created keeps reorder-safety). OpenAPI: `/v1` prefix all paths, `securitySchemes: bearerAuth`, `Problem` schema, 400/401/403/404/429 responses on all ops, Idempotency-Key header, Location header on 202, cursor-pagination example on a documented (not implemented) list endpoint pattern in spec comments → actually add `GET /v1/orders?cursor&limit` implemented against projection (small, makes pagination convention real + testable).
- [ ] `just oapi` recipe (oapi-codegen regen) + `gen` recipe globs sqlc.yaml files.
- [ ] README mermaid fixed (now true). Commit: `feat(gateway): /v1 + pending projection row + Location; OpenAPI models auth/errors/pagination`

### Lane L6 — observability

### Task 2.12: per-route RED
**Files:** Create `platform/web/httpserver/routetag.go`, Modify `middleware.go`, Test `routetag_test.go`
- [ ] Failing test: request to `/v1/orders/123` → span name `GET /v1/orders/{id}`, metric `http.server.duration{http.route="/v1/orders/{id}"}`, access log field `route` = pattern (not raw path).
- [ ] Impl: post-routing chi middleware reads `chi.RouteContext(ctx).RoutePattern()` → `span.SetName`, `http.route` attr, duration histogram, access-log uses pattern.
- [ ] Commit: `feat(httpserver): route-pattern span names + RED metrics — was single "http.server" series`

### Task 2.13: trace_id in logs everywhere
**Files:** Modify `platform/cqrs/logging.go`, `platform/web/httpserver/middleware.go` (AccessLog, Recover), examples ordering (`get_order.go` etc.), `platform/cqrs/cqrs.go` godoc, Tests `logging_test.go`
- [ ] Failing test: CQRS handler inside span → log records carry trace_id/span_id; access log line carries trace_id.
- [ ] Impl: all `logger.X(...)` → `XContext(ctx, ...)` in cqrs Logging + AccessLog + Recover; example pipelines reorder Tracing OUTERMOST; godoc documents required order.
- [ ] Commit: `fix(observability): context-aware logging — trace correlation actually works`

### Task 2.14: sampler config + exemplars + Pyroscope SDK
**Files:** Modify `platform/observability/telemetry/telemetry.go`, `examples/servicekit/service.go`, `cmd/skeleton/main.go`, go.mod, docs/operations.md, Test telemetry_test
- [ ] `TELEMETRY_TRACE_RATIO` (default 1.0) → explicit `ParentBased(TraceIDRatioBased)`; exemplar filter doc (`OTEL_METRICS_EXEMPLAR_FILTER=trace_based`) + enable on histograms; Pyroscope: `grafana/pyroscope-go` opt-in via `PYROSCOPE_ADDR` in servicekit (closes shelfware gap). Tests: sampler ratio applied; pyroscope no-op when unset.
- [ ] `/healthz` public-server placement: verify LB health route NOT behind auth (gateway mounts health before auth group; fix if behind — test asserts 200 unauthenticated).
- [ ] Commit: `feat(observability): sampling config, exemplars, pyroscope SDK wiring, unauthenticated healthz`

---

## Phase 3 — parallel lanes: L7 choreography, L8 platform polish

L7 depends on L5 (consume.Typed, serde) — start after Phase 2 merge.

### Lane L7 — choreography failure path

### Task 3.1: PaymentFailed event + deterministic demo rule
**Files:** Modify `proto/orders/v1/orders.proto` (+regen), `examples/payments/internal/app/process_payment.go`, gateway projection, notifications, Test payments + projection tests
- [ ] Failing tests: payment with `amount_cents >= 1_000_000` → `PaymentFailed{reason:"declined"}` emitted via outbox; projection → status `payment_failed`; notifications logs failure notification.
- [ ] Commit: `feat(payments): PaymentFailed event — choreography finally has a failure path`

### Task 3.2: correlation/causation IDs
**Files:** Modify `platform/messaging/consume/consume.go`, `outbox` enqueue path, gateway producer, Test e2e assertion
- [ ] Failing test: e2e — all events in one order's chain share `correlation-id` == original command id; each `causation-id` == parent message id.
- [ ] Impl: consume.Typed puts correlation/causation into ctx; outbox Enqueue auto-stamps headers from ctx (explicit override possible); gateway seeds correlation = command id.
- [ ] Commit: `feat(messaging): correlation/causation id propagation through consume→outbox chain`

### Task 3.3: unpaid-order deadline watcher
**Files:** Create `examples/orders/internal/app/unpaid_watcher.go`, Modify orders wiring, proto (`OrderPaymentTimedOut`), projection, Test orders integration (short timeout)
- [ ] Failing test: order created, no payment, T=2s → `OrderPaymentTimedOut` emitted once (idempotent re-poll), projection → `payment_timeout`.
- [ ] Impl: worker polls `status=created AND created_at < now()-T AND timeout_emitted=false` → outbox enqueue + mark, single-active via same advisory-lock helper. Wired via servicekit worker API (Task 4.2 AddWorker; until then goroutines adder).
- [ ] Commit: `feat(orders): payment-deadline watcher — stuck orders surface as events, not silence`

### Task 3.4: replay/redrive tooling + retention
**Files:** Create `cmd/redrive/main.go`, Modify ops docs, Test redrive integration test
- [ ] Failing test: DLT record with orig-topic header → `redrive` republished to orig topic with headers preserved, attempt counter reset.
- [ ] Impl: small CLI: `redrive --brokers --dlt orders.commands.dlt [--filter-header k=v] [--dry-run]`. Reads DLT, republishes, commits. Inbox-bypass replay documented (replay = new message-id mode `--fresh-ids` for projection rebuild vs dedup).
- [ ] Commit: `feat(ops): DLT redrive CLI + replay modes`

### Task 3.5: projection split seam
**Files:** Modify gateway layout: `examples/gateway/cmd/projection/main.go` (new optional binary), gateway wiring split, docs
- [ ] Projection package compiles standalone: `cmd/projection` runs consumer-only (servicekit, no public HTTP); gateway binary keeps embedded mode default (`GATEWAY_EMBEDDED_PROJECTION=true`). Test: build both; e2e green in embedded mode; integration smoke for standalone.
- [ ] Commit: `feat(gateway): projection extractable as standalone binary — edge/read-model split seam`

### Lane L8 — platform polish

### Task 3.6: cqrs Deadline behavior + StandardPipeline + resilience integration
**Files:** Create `platform/cqrs/deadline.go`, `platform/cqrs/pipeline.go`, Modify `platform/resilience/resilience.go`, examples handlers, Tests cqrs
- [ ] Failing tests: Deadline wraps handler ctx with timeout → DeadlineExceeded surfaces as typed err; `cqrs.StandardPipeline[C,R]("name", opts...)` returns Tracing→Logging→Metrics→Validation stack in correct order — examples' DecorateX funcs collapse to one line; resilience jitter test: full-jitter delay ∈ [0, backoff] (fix WithJitterFactor misuse); remove shadowed `resilience.RateLimiter` (deprecated note).
- [ ] Commit: `feat(cqrs): Deadline behavior + StandardPipeline — kills per-handler decorate boilerplate; fix retry jitter`

### Task 3.7: config Validate/Secret, closer, pg timeouts
**Files:** Modify `platform/config/config.go`, Create `platform/config/secret.go`, Modify `platform/run/closer.go`, `platform/storage/pg/config.go`, `pool.go`, gateway/servicekit configs, Tests each pkg
- [ ] Failing tests: (a) config struct with `Validate() error` returning err → Load fails; (b) `Secret` logs/prints as `[REDACTED]`, env-parses, `Reveal()` returns value; (c) Closer: late Add error logged (capture handler); item after slow teardown gets ctx.Err checked → remaining run with per-item budget slice; (d) pg: `PG_READER_MAX_CONNS` honored, ConnectTimeout set, default `statement_timeout` applied (assert via SHOW), pool-acquire bounded.
- [ ] Apply `Secret` to S3/redis/keycloak creds in configs. `FromContextRead` design note in pg godoc (writer-fallback hazard).
- [ ] Commit: `feat(platform): config Validate+Secret, closer budgets, pg reader sizing+timeouts`

### Task 3.8: migrations hardening
**Files:** Modify `platform/storage/pg/migrate.go`, `examples/servicekit/service.go`, `justfile`, ci.yml, docs/operations.md, Test migrate_test
- [ ] Failing test: Migrate uses dedicated conn (not pool round-robin) for advisory lock; `PG_MIGRATE_URL` override honored.
- [ ] Impl: dedicated `pool.Acquire`'d conn holds session lock for whole run (already mutex-guarded in-process; cross-process now safe on one conn); doc REQUIREMENT: direct-Postgres DSN for migrations behind PgBouncer (`PG_MIGRATE_URL`). servicekit `MIGRATE_ON_START` (default true, doc prod=migrate job); `just migrate <svc>` recipe. CI: squawk lint on `**/migrations/*.sql` (download-binary step), `-- +goose NO TRANSACTION` + CONCURRENTLY pattern documented.
- [ ] Commit: `fix(pg): migration lock on dedicated conn + PG_MIGRATE_URL; MIGRATE_ON_START opt-out; squawk in CI`

### Task 3.9: admin bind fatal
**Files:** Modify `examples/servicekit/lifecycle.go`, Test lifecycle test
- [ ] Failing test: occupied admin port → `servicekit.Run` returns error (not warn-and-continue); `ADMIN_BIND_OPTIONAL=true` → old behavior.
- [ ] Commit: `fix(servicekit): admin bind failure fatal by default — no more half-alive pods`

---

## Phase 4 — structural DX (SEQUENTIAL — path moves)

### Task 4.1: servicekit → platform/servicekit
**Files:** `git mv examples/servicekit platform/servicekit`, import rewrite (anchored sed per SP10 method), `internal/arch/arch_test.go`, docs refs
- [ ] Verify servicekit imports nothing from examples (`go list -deps`); move; sed `go-boilerplate/examples/servicekit` → `go-boilerplate/platform/servicekit`; goimports/gofumpt; arch test still passes (platform↛examples).
- [ ] Build+lint+test green. Commit: `refactor: promote servicekit to platform/ — harness no longer in delete-me zone`

### Task 4.2: servicekit flexibility
**Files:** Modify `platform/servicekit/service.go`, `config.go`, Create `options.go`, rebuild `cmd/skeleton/main.go` on servicekit, Tests servicekit
- [ ] Failing tests: `New(ctx, cfg, servicekit.WithoutKafka())` → no kafka client, no topic ensure; `WithoutPG()` → no pool/migrations; `AddWorker("cron", fn)` runs + stops via closer; `AddHTTPServer("public", srv)` lifecycle managed (replaces gateway hand-roll).
- [ ] skeleton rebuilt: `servicekit.New(WithoutKafka(), WithoutPG())` + HTTP — ONE wiring pattern repo-wide; skeleton e2e suite still green.
- [ ] Commit: `feat(servicekit): WithoutKafka/WithoutPG/AddWorker/AddHTTPServer; skeleton rebuilt on servicekit`

### Task 4.3: servicekit.Main + dedupe mains
**Files:** Create `platform/servicekit/main.go`, Modify 4 services' main.go, delete `WithLogWriter` stubs, Test via build + e2e
- [ ] `servicekit.Main(build func(ctx context.Context) (*servicekit.Service, error))` — automaxprocs, config load, run.Run, exit codes. 4 mains → ~5 lines each; double-teardown bug (orders main.go:37-40 Stop-after-run) dies with the rewrite; `WithLogWriter` dead stubs deleted.
- [ ] e2e green. Commit: `refactor(examples): servicekit.Main collapses service mains; drop dead WithLogWriter`

### Task 4.4: scaffolding + rename
**Files:** Create `scripts/new-service.sh`, `scripts/rename-module.sh`, Modify `justfile` (recipes `new-service`, `rename-module`, glob sqlc in `gen`), CONTRIBUTING.md
- [ ] `just new-service foo`: copies payments template, seds names/module path, prints checklist (CI matrix, compose block, .env). Test: run it in CI-style smoke — generated service builds (`scripts/new-service.sh footest && go build ./examples/footest/... && rm -rf`). Add as `just test-scaffold` + CI step.
- [ ] `just rename-module github.com/org/repo`: seds go.mod, imports, `goimports -local`, `.golangci.yml`, lefthook, justfile, Docker image names. Smoke: rename to tmp value in worktree → build green → revert (script self-test mode `--check`).
- [ ] Commit: `feat(dx): new-service + rename-module scaffolding — highest-leverage adoption fixes`

### Task 4.5: fakes.Broker
**Files:** Create `platform/testkit/fakes/broker.go`, Test `broker_test.go`
- [ ] In-memory topic→HandlerFunc dispatch implementing outbox `Publisher` + driving `kafka.HandlerFunc`/`consume.Typed` without Docker: `b.Produce(topic, rec)`, `b.Subscribe(topic, handler)`, sync delivery, header support. Goleak-clean.
- [ ] Commit: `feat(testkit): fakes.Broker — in-memory kafka substitute for fast-lane consumer tests`

### Task 4.6: fast-lane tests for example services
**Files:** Create unit tests in orders/payments/gateway/notifications app packages (no Docker), using fakes.Broker + fake stores
- [ ] Per service ≥2 `-short` tests: orders CreateOrderHandler (fake publisher → outbox enqueued, correlation stamped), payments threshold rule (success + PaymentFailed), gateway projection reorder-safety (paid-before-created upsert), notifications event handling. `just test-unit` covers them; pyramid real where copied.
- [ ] Commit: `test(examples): fast-lane unit tests — template services now model the pyramid`

### Task 4.7: arch invariants extended
**Files:** Modify `internal/arch/arch_test.go`
- [ ] New guards: examples don't import each other (gateway↛orders etc.); service `internal/` not imported cross-service; `testkit` imported only from `_test.go` files. Each guard proven by temporary violation (manual check during dev, not committed).
- [ ] Commit: `test(arch): cross-service + testkit import guards`

---

## Phase 5 — docs, deploy, final verify (SEQUENTIAL)

### Task 5.1: docs truth pass
**Files:** Rewrite `docs/adding-a-service.md` (servicekit-based, against REAL API, references `just new-service`), Modify README (automaxprocs row, diagram already fixed 2.11, stack table MinIO→SeaweedFS/aws-sdk, Go 1.26), `docs/testing.md` (nightly claim, e2e provisioning), `justfile:56` comment, `docs/operations.md:174` env-config claim + convert e2e to `t.Setenv`; DELETE `plan.md` → `docs/history/plan-2026-06-08.md`; move `docs/superpowers/` plans+specs → `docs/history/` with README disclaimer (keep THIS plan in place until done)
- [ ] Doc-code drift test where cheap: `adding-a-service.md` code blocks extracted + compiled in CI (script `scripts/doc-test.sh`, non-blocking acceptable → make blocking).
- [ ] Commit: `docs: adding-a-service rewritten against real API; truth pass on README/testing/operations; archive plan.md`

### Task 5.2: ADRs 0007–0013
**Files:** Create `docs/adr/0007-choreography-over-orchestration.md` (+ revisit triggers, correlation ids), `0008-projection-in-gateway.md` (+ split seam), `0009-db-per-service-one-instance-dev-topology.md`, `0010-rest-only-edge-no-sync-rpc.md` (ConnectRPC = blessed internal-RPC when needed), `0011-state-based-not-event-sourcing.md` (+ edge direct-produce note from finding A9, analytics = CDC-to-warehouse recommendation), amend `0004` (LISTEN/NOTIFY + logical-replication middle tier before Debezium; platform template-vs-library: TEMPLATE, forks diverge — recorded), `0012` already (MinIO, task 1.11), `0013-kip932-watch.md` (share groups replace custom retry when broker fleet 4.2+ + Redpanda parity)
- [ ] Commit: `docs(adr): record the load-bearing architecture decisions (0007-0013)`

### Task 5.3: ops runbooks + deploy reference
**Files:** Modify `docs/operations.md`: real DLT-redrive runbook (`cmd/redrive` + rpk inspect examples), projection-rebuild/replay procedure, safe-migration guide (migrate job, CONCURRENTLY/NO TRANSACTION, squawk), backup/DR section (PITR, outbox/inbox/offset consistency after restore, single-pg = dev topology note), per-component scaling table (gateway N replicas, consumer groups vs partitions, relay single-active). Create `deploy/k8s/` minimal reference manifests (gateway Deployment+Service+HPA sketch, migrate Job, probes wired to /livez /readyz, preStop drain) — reference quality, not prod-complete, marked as such.
- [ ] Commit: `docs(ops): redrive/replay/migration/DR runbooks + reference k8s manifests`

### Task 5.4: PGO pipeline
**Files:** Modify `justfile` (`pgo-fetch` recipe: pull profile from pyroscope API → `cmd/<svc>/default.pgo`), `.goreleaser.yaml` (uses default.pgo when present), docs/operations.md PGO section
- [ ] Commit: `build: PGO hooks — pyroscope profile → default.pgo`

### Task 5.5: FINAL VERIFICATION (verification-before-completion skill)
- [ ] `just lint` → 0 issues; `gofumpt -l .` → empty
- [ ] `go build ./... && go vet ./...`
- [ ] `go test ./... -race -count=1` (full, Docker on) → green ×2 runs
- [ ] `just test-e2e` → green ×2 (flake check)
- [ ] `just test-unit` (fast lane) → green, <10s
- [ ] mocks reproducible (`just gen-mocks && git diff --exit-code`)
- [ ] compose configs valid (`docker compose config -q` all profiles)
- [ ] arch tests green
- [ ] Final review: dispatch code-reviewer over full diff vs pre-plan main; fix MUST-FIX findings; update memory file.

---

## Execution notes

- Worktree lanes: `superpowers:using-git-worktrees`; one subagent per lane; merge order as stated per phase; I run integration suite after each merge.
- Every task: TDD (failing test first), commit per task minimum.
- Integration tests need Docker — verify daemon up before lanes start.
- On any test failure during build: backprop skill → consider new invariant.
- Caveman boundaries: code/commits/PR text = normal English.
