# Round 3 — Gateway Scale Fixes

> Source: gateway scaling-boundary analysis + AWS hosting research (2026-06-10).
> TDD for behavior changes; one commit per task; normal-English messages +
> "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.

Findings being fixed:
- F1: SSE = 1 dedicated redis conn per stream → silent hard wall at 1024 streams/replica (rueidis BlockingPoolSize), ~10k global (redis maxclients). Failure mode: heartbeats flow, updates never arrive, no signal.
- F2: shared PG writer ceiling ~2-3.5k orders/s independent of replicas: POST does sync pending INSERT + idempotency SELECT on WRITER; projection adds 3 tx/order. Split mode ≠ pure edge (ADR-0008 overstates).
- F3: PG connection wall at N≈4 replicas (25 conns × 4 = default max_connections=100).
- F4: docs gaps (SSE absent from scaling table; ADR-0008 honesty; conn arithmetic not in ops table).

## T1 — SSE shared subscription (F1)  [lane A]
Replace per-stream `Dedicate()` with ONE subscriber per Streamer (replica):
- Notify → PUBLISH single broadcast channel `orders:status` (payload JSON `{"order_id":"…","status":"…"}`); keep per-order channel publish REMOVED (one channel; sharding note for extreme scale in godoc: switch to `orders:status:{N}` hash-sharded channels — documented, not built).
- Streamer runs one subscriber goroutine (start on first stream or at New; lifecycle via Shutdown): subscribe loop mirrors cache/invalidation.go pattern — dedicated conn via Dedicate+SetPubSubHooks ack, resubscribe on drop, ON GAP → broadcast "refresh" to ALL registered streams (each re-reads currentStatus from store; equivalent of InvalidateAll).
- In-process registry: map[orderID]map[*stream]chan — register/unregister per request; delivery COALESCES to latest status (only latest matters, ordinals monotone): per-stream chan cap 1, overwrite semantics (drop stale, keep newest).
- Per-stream redis cost: ZERO extra conns. Poll fallback stays for no-redis mode and while subscriber down (subscriber-down flag → streams poll; or refresh-on-reconnect covers; pick simplest correct).
- ready-ack semantics preserved: stream's first snapshot read AFTER (a) registry registration AND (b) subscriber confirmed live (or poll mode) — same no-lost-terminal guarantee as today, now replica-level.
- Tests (existing 4 SSE tests stay green; new): (1) 1500 concurrent streams on one Streamer all receive a status update (proves wall gone; assert redis conn count didn't grow with streams — via rueidis nothing per-stream: structural + CLIENT LIST count < 50); (2) subscriber gap → streams receive refreshed status (no silent stall); (3) coalescing: rapid created→paid delivers paid (ordinal monotone, no stale regress); (4) goleak.
- docs: operations.md SSE row (one conn/replica; streams bounded by memory+FDs, not redis), sse package doc rewrite.

## T2 — write-path relief (F2)  [lane B]
- Idempotency reads (key-lookup + body-mismatch SELECT) → READER pool (`pg.FromContextRead`); bounded staleness documented: a duplicate within replica lag returns the same deterministic 202 (id identical — inbox dedups downstream); a body-mismatch missed within lag returns 202 instead of 409 — acceptable, noted in openapi description + code comment.
- Pending INSERT: stays sync by default (read-your-writes UX invariant). Add `GATEWAY_PENDING_ASYNC` (default false): true → insert moves to a buffered async writer (single goroutine, batched multi-row INSERT ON CONFLICT DO NOTHING, flush ≤50ms/≤100 rows, drop+WARN on full buffer — row is best-effort already; GET serves 404→client retries, SSE snapshot covers). Wired via servicekit worker; drains on shutdown.
- Tests: reader-routing asserted (idempotency works against reader pool); async mode: POST burst → rows eventually present, GET-after-POST may 404 (test tolerates), e2e stays on sync default.
- ADR-0008 amendment: split mode is NOT a pure edge — POST writes pending row + idempotency read on the read-model DB by design (read-your-writes); the aggregate writer is the scaling ceiling; levers = reader split, GATEWAY_PENDING_ASYNC, partitioning, Aurora.

## T3 — PG connection budget (F3)  [lane B]
- deploy/k8s/gateway-deployment.yaml: PG_MAX_CONNS=10 + comment with the formula (N_replicas × (writer+reader pools) + projection + migrate ≤ max_connections − reserved).
- docker-compose: `pgbouncer` service under new profile `pgbouncer` (edoburu/pgbouncer or bitnami; transaction mode, max_client_conn, auth passthrough for the 4 service DBs) + example envs in .env.example (PG_DSN via pgbouncer:6432 + PG_STATEMENT_CACHE_MODE=describe + PG_MIGRATE_URL direct) — opt-in, default profile unchanged.
- operations.md scaling table: connection arithmetic row (the N≈4 wall + formula + both remedies).
- Smoke: compose config -q with profile; optional live smoke `docker compose --profile pgbouncer up` pgbouncer + psql through it (do if cheap).

## T4 — docs honesty (F4)  [lane B, partly in T2/T3 commits]
- operations.md: SSE row (post-T1 semantics), conn arithmetic (T3), write-path ceiling note with the 4-commits-per-order breakdown.
- ADR-0008 amendment (T2).
- AWS research takeaways → new docs/aws-notes.md (condensed: MSK Provisioned not Serverless (EOS unverified), no RDS Proxy (session advisory locks pin), ElastiCache Valkey node-based not Serverless (CLIENT TRACKING/PSUBSCRIBE restricted — and why SSE uses a single non-pattern channel), ALB idle timeout ≥300s + dereg delay vs SSE, KEDA MSK scaler, links).

Final: lint 0, short green, `go test -p 1 ./examples/gateway/... ./examples/e2e/ ./platform/... -count=1` green, review pass over diff, plan archived, memory updated.
