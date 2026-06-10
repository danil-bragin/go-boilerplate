# SP11: Kafka-Native Highload + Edge Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`). Mature codebase — read existing patterns (`platform/messaging/kafka`, `examples/servicekit`, `platform/web/httpserver`) before writing; verify franz-go APIs with `go doc`; TDD; keep `golangci-lint run` 0 + gofumpt clean + tests green. lefthook pre-commit runs build+fmt+lint. Report deviations.

**Goal:** Non-blocking retry-topics redrive, Kafka EOS transactional helper, and per-IP/distributed rate-limit — all franz-go/Kafka-native (Watermill rejected).

**Architecture:** (A) `platform/messaging/retry` — tiered retry topics (`<base>.retry.<dur>` → DLT) with due-time headers + pause/resume partition waiting; harness opt-in via `RetryPolicy`. (B) `kafka.TransactConsumer` over `kgo.GroupTransactSession` for kafka→kafka exactly-once. (C) `platform/web/ratelimit` — `Limiter` interface, in-memory + Redis(rueidis Lua) impls; `RateLimitPer` middleware with trusted-proxy IP extraction; gateway swap.

**Tech Stack:** franz-go (kgo PauseFetchPartitions/GroupTransactSession) · rueidis (Lua token bucket) · x/time/rate · testcontainers (redpanda/redis).

**Spec:** `docs/superpowers/specs/2026-06-09-kafka-highload-edge-design.md`.

---

## Task 1: `platform/messaging/retry` — core types + escalation (producer side)

**Files:** Create `platform/messaging/retry/retry.go` (policy, topic naming, headers), `platform/messaging/retry/escalate.go` (escalation publisher), `platform/messaging/retry/retry_test.go`.

Read first: `platform/messaging/kafka/dlq.go` (existing WithRetry/DLT conventions, `kafka.Record`, header helpers), `platform/messaging/kafka/producer.go` (Producer iface/Produce), `platform/messaging/kafka/handler.go` or equivalent (`kafka.HandlerFunc` signature).

- [ ] **Step 1: types + naming (TDD).** `retry.go`:
  ```go
  type Policy struct {
      Tiers        []time.Duration // default [5s,30s,5m]
      FastAttempts int             // in-process attempts before escalation, default 1
  }
  func DefaultPolicy() Policy
  func TierTopic(base string, tier int, d time.Duration) string // "orders.commands.retry.5s" (use d.String())
  func DLTTopic(base string) string                              // "<base>.DLT" — reuse/delegate to existing dlq naming if exported
  ```
  Header constants: `HeaderAttempt = "retry-attempt"`, `HeaderOrigTopic = "retry-orig-topic"`, `HeaderDueAt = "retry-due-at"` (unix-ms string), `HeaderLastError = "retry-last-error"` (truncate 512B). Helpers `setRetryHeaders(rec *kafka.Record, attempt int, orig string, due time.Time, err error)` + `parseRetryHeaders(rec) (attempt int, orig string, due time.Time, ok bool)`.
  Failing tests first: naming for each tier; header round-trip (set→parse); parse-missing→!ok; error truncation.
- [ ] **Step 2: escalation.** `escalate.go`:
  ```go
  // Escalator routes a failed record to the next retry tier, or DLT after the last.
  type Escalator struct { prod kafka.Producer /* match real iface */ ; policy Policy }
  func NewEscalator(prod /*…*/, policy Policy) *Escalator
  // Escalate publishes rec to the tier after its current retry-attempt (0 = first failure → tier 0).
  // Preserves key, value, original headers; sets retry headers; returns the destination topic.
  func (e *Escalator) Escalate(ctx context.Context, origTopic string, rec kafka.Record, cause error) (string, error)
  ```
  Logic: attempt := parsed attempt (0 if absent); if attempt >= len(Tiers) → publish to DLT (reuse existing DLT publish/headers if dlq.go exports it; else same shape) ; else publish to `TierTopic(orig, attempt, Tiers[attempt])` with `retry-attempt = attempt+1`, due = now+Tiers[attempt]. Unit-test with the moq `PublisherMock`?? — NO: producer iface is kafka-internal; use a small fake producer in-package capturing records. Tests: first failure→tier0+headers; tier walk; after last→DLT; key/value/original-headers preserved.
- [ ] **Step 3:** `go test -race ./platform/messaging/retry/...` PASS; build/vet/lint/gofumpt clean repo-wide.
- [ ] **Step 4: Commit** `feat(retry): tiered retry-topic policy, naming, headers, escalator`.

## Task 2: `retry.Consumer` — due-time wait via pause/resume + redelivery

**Files:** Create `platform/messaging/retry/consumer.go`, `platform/messaging/retry/consumer_test.go` (unit w/ fakes where possible), `platform/messaging/retry/integration_test.go` (testcontainers redpanda, `testing.Short()` skip).

Read first: `platform/messaging/kafka/consumer.go` + `run.go` (NewConsumer opts, poll loop, commit pattern, Close(ctx)), `platform/messaging/kafka/kafkatest/` (NewRedpanda helper), franz-go docs: `go doc github.com/twmb/franz-go/pkg/kgo Client.PauseFetchPartitions` / `ResumeFetchPartitions` / `PollFetches`.

- [ ] **Step 1: consumer.** `consumer.go`:
  ```go
  // Consumer consumes all retry tiers for a set of base topics and redrives
  // due records into the original handler. Not-yet-due records pause their
  // partition until due (no busy-wait; per-tier delay is uniform so
  // head-of-line wait ≤ tier delay).
  type Consumer struct { /* kgo client, escalator, handler, policy, logger/onError */ }
  func NewConsumer(cfg kafka.Config, group string, baseTopics []string, handler kafka.HandlerFunc, esc *Escalator, policy Policy, opts ...Option) (*Consumer, error)
  func (c *Consumer) Run(ctx context.Context) error
  func (c *Consumer) Close(ctx context.Context) error
  ```
  Subscribes `TierTopic(b, i, d)` ∀ base × tier. Group `<group>` (caller passes e.g. "orders.retry"). Poll loop (mirror kafka/run.go shape): per record — parse due; if `now < due`: `client.PauseFetchPartitions(map[string][]int32{topic:{part}})`, schedule `time.AfterFunc(until, resume)` (track paused set, mutex; resume via `ResumeFetchPartitions`), do NOT process/commit this record now — seek back? NO: franz-go pause keeps the fetched-but-unprocessed records? VERIFY semantics: after pause, records already polled are in hand — to avoid processing early, the consumer must NOT commit and must re-fetch after resume. Correct approach (document + implement): on not-due record, pause partition + `client.SetOffsets`/use `kgo.EpochOffset` to rewind the partition to that record's offset so it re-fetches after resume (verify exact API: `client.SetOffsets(map[string]map[int32]kgo.EpochOffset{...})` on a group consumer — if SetOffsets is incompatible with group management, alternative: buffer the not-due records in-memory with their offsets and process them when due via the timer, committing only after processing, while the partition stays paused to bound memory). PICK whichever franz-go supports cleanly for group consumers, implement ONE, document why. The buffered approach is likely simpler + safe: pause partition (stops more fetches), hold the in-hand records, timer fires at due → process → commit → resume. Memory bounded: one poll's worth per paused partition.
  - Due record path: call `handler(ctx, rec)`; success → mark for commit; failure → `esc.Escalate(...)` then mark for commit (escalated = handled).
  - Commit batch per poll for processed records (mirror existing batch-commit pattern).
- [ ] **Step 2: unit tests** (no broker): due-parse routing logic + paused-buffer bookkeeping if factored into a testable struct; keep thin.
- [ ] **Step 3: integration test** (redpanda, skip under -short):
  - `TestRetry_RedeliversAfterDelay`: handler fails on first delivery of msg A (count via map), Escalator wired with Tiers=[2s]; produce A to base topic; main consumer (kafka.NewConsumer + wrap: on failure → escalate, commit) ; retry.Consumer with the same handler. Assert: A processed successfully ~2s later (second delivery via retry tier), exactly-once side effect with inbox-style dedup map.
  - `TestRetry_PartitionNotBlocked`: handler fails A (escalates) then B arrives on same partition; assert B processes immediately (<1s), long before A's tier delay elapses. THE non-blocking proof.
  - `TestRetry_PoisonWalksTiersToDLT`: Tiers=[1s,1s]; handler always fails for A; assert A lands in `<base>.DLT` with retry-attempt=2 within ~5s; consume DLT to verify headers.
- [ ] **Step 4:** race + lint + gofumpt clean. `go test -race ./platform/messaging/retry/... -count=1` (integration w/ Docker) PASS.
- [ ] **Step 5: Commit** `feat(retry): retry consumer with due-time pause/resume redelivery and DLT terminal`.

## Task 3: harness `RetryPolicy` + wire orders

**Files:** Modify `examples/servicekit/consumers.go` (+ `config.go` if env knobs), `examples/orders/orders.go` (or wherever orders calls AddConsumer), `examples/servicekit/servicekit_test.go` (extend), orders integration test extend.

Read first: `examples/servicekit/consumers.go` (AddConsumer + WithRetry wrap + EnsureTopics incl DLT), `examples/orders/*.go` (consumer wiring).

- [ ] **Step 1:** add to servicekit:
  ```go
  // AddConsumerWithRetry wires a consumer whose failures escalate to tiered
  // retry topics (non-blocking) instead of in-process backoff. nil-policy
  // callers should use AddConsumer (unchanged behavior).
  func (s *Service) AddConsumerWithRetry(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc, policy retry.Policy) error
  ```
  Implementation: EnsureTopics(base + every TierTopic + DLT); build Escalator from s.producer; main-consumer wrap = `policy.FastAttempts` immediate in-process attempts (reuse kafka.WithRetry with attempts=FastAttempts and a custom on-exhaust → Escalate + return nil/commit — read dlq.go to see if WithRetry supports an escalation hook; if not, write the small wrap inline here) ; register retry.Consumer (group `groupID+".retry"`) as a goroutine like other consumers; closer registration mirrors AddConsumer.
- [ ] **Step 2:** orders switches its command consumer to `AddConsumerWithRetry(..., retry.DefaultPolicy())`. payments/notifications/gateway stay on AddConsumer (demonstrates both modes).
- [ ] **Step 3:** servicekit unit/short tests still green; orders integration test still green (its happy path unaffected); ADD one orders-level (or servicekit-level) integration assertion if cheap — else rely on Task 2's package-level integration tests.
- [ ] **Step 4:** e2e green (`go test -count=1 -timeout 300s ./examples/e2e/...`). build/lint/gofumpt clean.
- [ ] **Step 5: Commit** `feat(servicekit): opt-in retry-topic policy; orders uses non-blocking redrive`.

## Task 4: Kafka EOS — `kafka.TransactConsumer`

**Files:** Create `platform/messaging/kafka/transact.go`, `platform/messaging/kafka/transact_test.go` (integration, redpanda, -short skip); create `docs/adr/0006-kafka-eos-boundaries.md`; modify `platform/messaging/kafka/consumer.go` (add `kgo.FetchIsolationLevel(kgo.ReadCommitted())` to NewConsumer opts — VERIFY no test breaks).

Read first: `go doc github.com/twmb/franz-go/pkg/kgo GroupTransactSession`, `NewGroupTransactSession`, `TransactionalID`, `RequireStableFetchOffsets`; existing `client.go`/`consumer.go` for Config/opt plumbing.

- [ ] **Step 1: transact.go.**
  ```go
  // TransactConsumer provides exactly-once consume-process-produce for pure
  // kafka→kafka pipelines via Kafka transactions. Paths touching a database
  // must use the outbox instead (a Kafka transaction cannot span the DB) —
  // see docs/adr/0006-kafka-eos-boundaries.md.
  type TransactConsumer struct { sess *kgo.GroupTransactSession /* + cfg */ }
  func NewTransactConsumer(cfg Config, txnID, groupID string, topics ...string) (*TransactConsumer, error)
  // ProcessFn returns records to produce within the same transaction.
  type ProcessFn func(ctx context.Context, rec Record) ([]Record, error)
  func (t *TransactConsumer) Run(ctx context.Context, fn ProcessFn) error
  func (t *TransactConsumer) Close(ctx context.Context) error
  ```
  NewGroupTransactSession opts: seeds/clientID from cfg (reuse the option builder), `kgo.TransactionalID(txnID)`, `kgo.ConsumerGroup(groupID)`, `kgo.ConsumeTopics(...)`, `kgo.FetchIsolationLevel(kgo.ReadCommitted())`, `kgo.RequireStableFetchOffsets()`. Run loop: `sess.Begin()` → `PollFetches` → iterate records → fn → produce returned records via the session client → `sess.End(ctx, kgo.TryCommit)`; on fn error or produce error → `sess.End(ctx, kgo.TryAbort)` + log + continue (records redeliver). Respect ctx cancel. VERIFY exact End signature/semantics with go doc; adapt.
- [ ] **Step 2: integration test** (redpanda — note in test if redpanda txn support needs a broker flag; kafkatest helper may need `--enable-idempotence`-like config — VERIFY by running; redpanda supports Kafka transactions by default in recent versions):
  - `TestTransact_ExactlyOnceHappyPath`: produce 5 msgs to in-topic; TransactConsumer doubles each into out-topic; read-committed consumer on out-topic sees exactly 5; offsets committed.
  - `TestTransact_AbortOnError`: fn fails on msg 3 first time → txn aborts → redelivery reprocesses; read-committed consumer sees NO duplicate for msgs 1-2 (their first-attempt produces were aborted with the txn) and eventually all 5 exactly once. (This validates atomicity: aborted batch's produces invisible.)
- [ ] **Step 3: NewConsumer ReadCommitted.** Add the opt; run the FULL kafka + messaging + e2e test suites — confirm zero behavior change for non-transactional topics (read-committed only filters aborted txn data; none exists on plain topics).
- [ ] **Step 4: ADR 0006**: EOS (kafka→kafka) vs outbox (DB→kafka) vs inbox (consumer dedup); why all three coexist; when to pick which; perf note (txn overhead per batch; default services stay on outbox).
- [ ] **Step 5:** race/lint/gofumpt clean; integration PASS; e2e green.
- [ ] **Step 6: Commit** `feat(kafka): EOS TransactConsumer (GroupTransactSession) + read-committed consumers + ADR-0006`.

## Task 5: `platform/web/ratelimit` — memory + Redis limiters

**Files:** Create `platform/web/ratelimit/ratelimit.go` (iface), `platform/web/ratelimit/memory.go`, `platform/web/ratelimit/redis.go`, `platform/web/ratelimit/ratelimit_test.go` (unit), `platform/web/ratelimit/redis_integration_test.go` (redis testcontainer, -short skip).

Read first: `platform/storage/cache/cache.go` (rueidis client usage pattern + testcontainer redis pattern in cache_test.go), `golang.org/x/time/rate` docs.

- [ ] **Step 1: iface + memory (TDD).**
  ```go
  type Limiter interface { Allow(ctx context.Context, key string) (bool, error) }

  // NewMemory: per-key token bucket, idle eviction. Single-instance scope.
  func NewMemory(rps float64, burst int, opts ...MemoryOption) *Memory  // opts: WithIdleTTL(d), WithMaxEntries(n)
  func (m *Memory) Allow(ctx context.Context, key string) (bool, error) // never errors
  func (m *Memory) Close()                                              // stops janitor
  ```
  Impl: `map[string]*entry{lim *rate.Limiter, lastSeen atomic}` + RWMutex; janitor goroutine evicts idle > TTL (default 10m); MaxEntries cap (default 100_000): on overflow evict oldest-seen (simple scan ok at this scale or random-sample eviction — pick one, comment). Failing tests first: per-key isolation (key A exhausted, key B allowed); burst honored; refill over time (use small rps + sleeps OR inject a clock — prefer injectable `now func() time.Time` for determinism); eviction after TTL; MaxEntries cap.
- [ ] **Step 2: redis.go.**
  ```go
  // NewRedis: distributed token bucket via atomic Lua on rueidis.
  // failOpen: on Redis error Allow returns (true, nil) + onError callback (default) — availability over strictness at the edge.
  func NewRedis(client rueidis.Client, rps float64, burst int, opts ...RedisOption) *Redis // opts: WithFailClosed(), WithOnError(f), WithKeyPrefix(s)
  ```
  Lua (EVALSHA, fallback EVAL on NOSCRIPT): key `rl:<key>` hash {tokens, ts_ms}; compute refill from elapsed*rps capped at burst; if tokens≥1 decrement+allow else deny; PEXPIRE idle (e.g. burst/rps*2 + 60s). Use rueidis `client.Do(ctx, client.B().Evalsha()...)` builder (mirror cache.go style).
- [ ] **Step 3: redis integration test** (testcontainer): single limiter allow/deny/burst; **two NewRedis instances (same client or two clients) share one budget** — N allowed across both = burst, proving distributed correctness; fail-open: close container → Allow returns true + onError fired (and FailClosed variant returns false).
- [ ] **Step 4:** race/lint/gofumpt; unit (-short) + integration PASS.
- [ ] **Step 5: Commit** `feat(ratelimit): per-key limiter (memory + distributed redis lua token bucket)`.

## Task 6: middleware + gateway wiring + docs + final verify

**Files:** Modify `platform/web/httpserver/security.go` (add `RateLimitPer` + `ClientIPKey`; deprecate-note on `RateLimit`), `platform/web/httpserver/security_test.go`; modify `examples/gateway/config.go`, `examples/gateway/deps.go`/`routes.go` (swap limiter); modify docs: `docs/ARCHITECTURE.md` (deferred list shrinks: retry-topics ✓ EOS ✓ per-IP ratelimit ✓), `platform/README.md` (messaging/retry + web/ratelimit lines), `docs/operations.md` (retry-topics runbook: tier topics, DLT redrive, lag on retry groups), `docs/conventions.md` if it lists packages.

- [ ] **Step 1: middleware (TDD).** In httpserver:
  ```go
  // ClientIPKey extracts the caller IP for rate-limit keying. RemoteAddr is
  // authoritative unless it is a trusted proxy, in which case the closest
  // untrusted hop from X-Forwarded-For is used. Never trust XFF from
  // untrusted peers (spoofable).
  func ClientIPKey(trusted []netip.Prefix) func(*http.Request) string
  func RateLimitPer(l ratelimit.Limiter, key func(*http.Request) string) func(http.Handler) http.Handler
  ```
  RateLimitPer: deny → 429 + `Retry-After: 1` + problem+json (match existing RateLimit's response shape); limiter error (fail-closed mode) → 429; key empty → fall back to RemoteAddr. Tests: per-IP isolation through the middleware (two RemoteAddrs, one exhausted, other 200); XFF spoof from UNtrusted peer ignored (key = RemoteAddr); XFF honored when RemoteAddr ∈ trusted CIDR (rightmost-untrusted hop logic: walk XFF right-to-left, skip trusted, take first untrusted); 429 body/headers. Add `Deprecated:` comment on the old global `RateLimit` pointing here (keep it working).
- [ ] **Step 2: gateway swap.** Config adds `RatelimitRPS float64 env:"RATELIMIT_RPS" envDefault:"50"`, `RatelimitBurst int env:"RATELIMIT_BURST" envDefault:"100"`, `TrustedProxies []string env:"TRUSTED_PROXIES" envSeparator:","`, `RatelimitRedis bool env:"RATELIMIT_REDIS" envDefault:"false"`. deps.go builds: if RatelimitRedis && redis (cache) client available → ratelimit.NewRedis(shared rueidis client) else ratelimit.NewMemory; parse TrustedProxies CIDRs (invalid → fail-fast config error). routes.go replaces `RateLimit(...)` with `RateLimitPer(limiter, ClientIPKey(trusted))`. Compose: add the env knobs to gateway service (defaults fine). Gateway tests: keep limits high (RPS 1000) so existing tests unaffected; add one test asserting per-IP 429 with tiny limits.
- [ ] **Step 3: docs** as listed in Files. plan.md decision log: add "Watermill evaluated → rejected (Sarama/SyncProducer/per-msg ack vs franz-go async batch; batteries already built)" one-liner.
- [ ] **Step 4: FINAL whole-repo verify:** `go build ./... && go vet ./...`; `golangci-lint run ./...` 0; gofumpt clean (non-generated); `go test -short ./...` green; arch guard green; mocks reproducible; `docker compose config` (all profiles) valid; **full integration**: `go test -count=1 ./platform/messaging/... ./platform/web/... ./examples/...` green; **e2e** `go test -count=1 -timeout 300s ./examples/e2e/...` green.
- [ ] **Step 5: Commit** `feat(gateway,web): per-IP rate-limit (memory/redis) with trusted-proxy IP extraction; docs`.

## Task 7: adversarial review + fixes
- [ ] Dispatch opus adversarial review: retry correctness (pause/resume + buffered-records memory bound, commit semantics, tier walk, inbox composition), EOS atomicity claims vs test evidence, ratelimit security (XFF spoofing, fail-open tradeoff, Lua atomicity), regressions (ReadCommitted on plain consumers, gateway behavior change), docs honesty.
- [ ] Fix all MUST-FIX findings; re-verify (build/lint/short/e2e). Commit fixes.

---

## Self-Review (completed)
- **Spec coverage:** A→T1-3, B→T4, C→T5-6, review→T7. Watermill-rejection note → T6 docs. All spec sections mapped.
- **Placeholders:** none. The one genuinely API-uncertain point (franz-go pause/resume + in-hand records semantics) is given two concrete candidate implementations with instruction to verify via go doc and pick one — a real engineering decision, not a TBD.
- **Type consistency:** `retry.Policy`/`Escalator`/`Consumer` names consistent T1→T2→T3; `ratelimit.Limiter` iface name consistent T5→T6; `kafka.Record`/`HandlerFunc` reused from existing package throughout.
