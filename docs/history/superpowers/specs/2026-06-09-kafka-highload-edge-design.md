# SP11: Kafka-Native Highload + Edge Hardening — Design

Status: approved (2026-06-09). Scope: three independent features — (A) retry-topics non-blocking redrive, (B) Kafka EOS transactional helper, (C) per-IP / distributed rate-limit. Watermill evaluated and REJECTED (Sarama-based, SyncProducer-only publish, per-message ack model — lateral move that trades throughput for batteries we already built on franz-go). Core stays 100% Kafka via franz-go.

## A. retry-topics — `platform/messaging/retry` (non-blocking redrive)

Problem: today `kafka.WithRetry` retries in-process with backoff then DLTs. During backoff the partition is BLOCKED (head-of-line). Highload Kafka pattern: tiered retry topics — failures move OFF the main topic immediately; the partition keeps flowing.

Design:
- **Tiers** configurable, default `[5s, 30s, 5m]`. Topic naming: `<base>.retry.<tier-index>` with the delay encoded in topic config/name (`orders.commands.retry.5s`, `.retry.30s`, `.retry.5m`). After the last tier → `<base>.DLT` (existing DLT convention).
- **Producer side (escalation):** when the main handler fails after N fast in-process attempts (N configurable, default 1 — one immediate retry for blips), publish the record to tier-1 retry topic with headers:
  - `retry-attempt` (int, increments per tier)
  - `retry-orig-topic` (the base topic)
  - `retry-due-at` (RFC3339/unix-ms: now + tier delay)
  - `retry-last-error` (truncated error string, diagnostics)
  - original key, value, and headers preserved.
  Then COMMIT the original record — the main partition is unblocked.
- **Retry consumer** (`retry.Consumer`): one per service, consumer group `<group>.retry`, subscribes ALL tiers of the topics that service consumes. Per record:
  - reads `retry-due-at`; if `now < due` → pause that partition (franz-go `PauseFetchPartitions`) and set a timer to resume (`ResumeFetchPartitions`) at due-time; do NOT commit; re-poll later. Within one tier the delay is uniform → bounded head-of-line (≤ tier delay), order-per-tier preserved. No busy-loop, no sleep-in-handler.
  - if due → invoke the ORIGINAL business handler (same `kafka.HandlerFunc`). Success → commit. Failure → escalate to next tier (or `.DLT` after last), commit.
- **Inbox composition:** redelivered records keep the same message-id → `inbox.ProcessOnce` dedups → effectively-once preserved across retries. Document this.
- **Harness integration:** `servicekit.AddConsumer` gains an optional `RetryPolicy{Tiers []time.Duration, FastAttempts int}` (nil → current behavior: in-process WithRetry → DLT, unchanged). When set: handler wrap = fast attempts → escalate-to-retry-topics; harness also launches the retry.Consumer and provisions tier topics + DLT via EnsureTopics.
- **Wire ONE example service with it** (orders — the reference service) to demonstrate; others keep the default (shows both modes).
- Tests: unit (escalation header math, tier progression); integration (testcontainers redpanda): fail-once handler → message lands in tier-1, redelivered after ~5s, succeeds, committed; poison message → walks all tiers → DLT; partition NOT blocked (a subsequent good message processes while a failed one waits in retry); inbox dedup across redelivery.

## B. Kafka EOS — transactional consume-process-produce (`platform/messaging/kafka`)

Scope: exactly-once for PURE kafka→kafka paths via Kafka transactions. The outbox stays the answer for any path touching Postgres (a Kafka txn cannot span the DB). EOS here is an OPT-IN capability + reference, not forced on the example services.

Design:
- `kafka.NewTransactConsumer(cfg, txnID string, topics ...string)` wrapping franz-go `kgo.GroupTransactSession` with: `kgo.TransactionalID(txnID)`, `kgo.FetchIsolationLevel(kgo.ReadCommitted())`, `kgo.RequireStableFetchOffsets()`, consumer group + cooperative-sticky (match existing consumer opts where compatible with transact sessions — VERIFY franz-go constraints; GroupTransactSession manages commit itself; BlockRebalanceOnPoll not applicable the same way).
- `Run(ctx, processFn)` loop: `sess.Begin()` → poll fetches → for each record call `processFn(ctx, record) ([]kafka.Record, error)` (returns records to produce) → produce them inside the txn → `sess.End(ctx, kgo.TryCommit)`. On process error → `sess.End(ctx, kgo.TryAbort)` → records redelivered (at-least-once within the aborted txn, exactly-once net effect for committed ones).
- Downstream consumers must read `ReadCommitted` to see EOS semantics — our consumer defaults get `FetchIsolationLevel(ReadCommitted)` added (safe for non-transactional topics too; VERIFY no behavior change for plain consumers).
- Tests (testcontainers redpanda — VERIFY redpanda supports Kafka transactions/EOS; it does since v21, but confirm in test): happy-path consume→produce committed atomically; abort path → no produced record visible to a read-committed consumer + offsets not committed (redelivery); duplicate-protection across session restart with same txn id.
- ADR `0006-kafka-eos-boundaries.md`: when EOS (kafka→kafka) vs outbox (DB↔kafka) vs inbox (consumer dedup); why all three coexist.
- Example: do NOT rewire the 4 services (they all touch PG → outbox is correct). Add a small runnable reference under `examples/testing` or a doc'd snippet + the integration test as the living example.

## C. per-IP / distributed rate-limit — `platform/web/ratelimit`

Problem: current `httpserver.RateLimit(rps, burst)` is one global bucket — a single noisy client starves everyone; and it's per-instance.

Design:
- New package `platform/web/ratelimit`:
  - `type Limiter interface { Allow(ctx context.Context, key string) (bool, error) }`
  - `NewMemory(rps float64, burst int, opts...)` — `map[key]*rate.Limiter` + mutex + idle eviction (last-seen TTL, default 10m, janitor goroutine; cap max entries ~100k LRU-ish to bound memory). Single-instance correct, zero deps.
  - `NewRedis(client rueidis.Client, rps float64, burst int, opts...)` — atomic token-bucket as a Lua script (EVALSHA; keys `rl:{key}`, fields tokens/ts, PEXPIRE idle TTL). Distributed-correct across replicas. Fail-open vs fail-closed configurable (default fail-open with a warn log + metric — availability over strictness at the edge; document).
- Middleware in `platform/web/httpserver`: `RateLimitPer(limiter ratelimit.Limiter, keyFn func(*http.Request) string)` → 429 + `Retry-After` on deny. Provide `ClientIPKey(trustedProxies []netip.Prefix)`: uses `RemoteAddr` unless the peer is a trusted proxy, then leftmost-untrusted `X-Forwarded-For` hop (NEVER trust XFF blindly — security note in doc + code comment). Keep the existing global `RateLimit` (deprecated note pointing at per-IP).
- Gateway wiring: replace global limiter with `RateLimitPer(memory-limiter, ClientIPKey(cfg.TrustedProxies))`; config `RATELIMIT_RPS`, `RATELIMIT_BURST`, `TRUSTED_PROXIES` (CIDR list), `RATELIMIT_REDIS` (bool — use the shared rueidis client when cache is configured; degrade to memory if Redis absent — consistent graceful-degradation pattern).
- Tests: unit (memory limiter per-key isolation, eviction, burst; XFF parsing trusted vs untrusted spoof rejected); integration (redis testcontainer: two limiter instances share state — N+M requests across "two replicas" enforce one budget); middleware 429 + Retry-After; gateway e2e unaffected (limits high in tests).

## Shared
- All packages: gofumpt/lint-0 bar, `-short` unit lane + testcontainers integration, docs updated (`docs/ARCHITECTURE.md` deferred-list shrinks; `platform/README.md` index gains retry + ratelimit; `docs/operations.md` retry-topics runbook note).
- No behavior change for existing users unless they opt in (RetryPolicy nil = old path; EOS opt-in; gateway rate-limit swap IS a behavior change — per-IP instead of global — documented, env-tunable).

## Out of scope
Delayed-message scheduling beyond tiered topics; cross-DB EOS (outbox covers); per-user/token rate-limit keys (keyFn is pluggable — IP ships); Watermill (rejected, ADR-worthy note added to plan.md decision log).

## Verification
Whole-repo: build/vet/lint 0, gofumpt clean, fast lane green, e2e green, new integration tests green (redpanda/redis), arch guard green, mocks reproducible, compose valid. Opus adversarial review at the end; fix findings.
