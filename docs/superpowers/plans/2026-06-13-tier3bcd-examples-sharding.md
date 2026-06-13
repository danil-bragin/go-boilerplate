# Tier-3 B/C/D — wire `pg.ShardedPool` into the examples — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **After EACH commit, run `git log --oneline -1` and confirm the new SHA — the pre-commit hook (build+fmt+lint) WILL reject unused params / `fmt.Errorf`-without-args / etc.; fix and re-commit until it lands. Do not report a commit as done without verifying it.**

**Goal:** Run the whole 4-service choreography on `pg.ShardedPool` so the write path scales `~M×` with `M` Postgres shards, while `M=1` (default, `PG_SHARDS` unset) stays byte-identical — every existing unit/integration/e2e/traffic/chaos test green unchanged.

**Architecture:** `servicekit` adopts `*pg.ShardedPool` uniformly (`M=1` from `PG_DSN`, `M≥1` from `PG_SHARDS`). The consumer middleware sets the shard key from the Kafka record key (always `order_id`); domain repos call `sp.FromContext(ctx)`/`sp.RunInTx(ctx,fn)`; relay/cleaners/audit run per physical shard; gateway keyed reads route, keyless reads (LIST, audit verify) fan out and merge.

**Tech Stack:** Go 1.26, pgx v5, the shipped `pg.ShardedPool` primitive (sub-project A), testcontainers, goose.

**Spec:** `docs/superpowers/specs/2026-06-13-tier3-bcd-examples-sharding-design.md`. Primitive: `docs/superpowers/specs/2026-06-13-tier3-shardedpool-primitive-design.md`. ADR-0019.

**Hard rules (every task preserves):**
1. `M=1` is byte-identical — the full existing suite stays green. This is the safety net; run it at each phase boundary.
2. No cross-shard transaction; order_id → one shard for life.
3. Effectively-once untouched (order write + outbox enqueue one tx on one shard).
4. Verify every commit lands (lint hook is strict).

**The mechanical recipe (used by several tasks):** a domain repo / handler that holds `pool *pg.Pool` and calls `pg.FromContext(ctx, pool)` / `pg.FromContextRead(ctx, pool)` becomes: hold `sp *pg.ShardedPool`; replace those calls with `sp.FromContext(ctx)` / `sp.FromContextRead(ctx)` (both now return `(pg.DBTX, error)` — handle the error: in a query method, `db, err := sp.FromContext(ctx); if err != nil { return ..., err }`). The shard key is already in ctx (set by the consumer middleware for the command/event path, or by the gateway at the edge for keyed reads).

---

## Phase B — servicekit core + consumer routing

### Task B1: `pg.Config.ToShardedConfig` + `PG_SHARDS` env

**Files:** Modify `platform/storage/pg/config.go`; test `platform/storage/pg/config_test.go`.

- [ ] **Step 1: failing test** (`config_test.go`, add)

```go
func TestConfig_ToShardedConfig_SingleVsMulti(t *testing.T) {
	// No PG_SHARDS ⇒ one shard from DSN (M=1, byte-identity).
	c := pg.Config{DSN: "dsn0", MaxConns: 7}
	sc := c.ToShardedConfig()
	require.Len(t, sc.DSNs, 1)
	require.Equal(t, "dsn0", sc.DSNs[0].Reveal())
	require.Equal(t, int32(7), sc.PerShard.MaxConns)

	// PG_SHARDS set ⇒ one shard per listed DSN.
	c2 := pg.Config{DSN: "dsn0", Shards: []config.Secret{"a", "b", "c"}}
	sc2 := c2.ToShardedConfig()
	require.Len(t, sc2.DSNs, 3)
	require.Equal(t, []string{"a", "b", "c"},
		[]string{sc2.DSNs[0].Reveal(), sc2.DSNs[1].Reveal(), sc2.DSNs[2].Reveal()})
}
```

(Use the real `config.Secret` type. Confirm whether the test package is `pg_test`.)

- [ ] **Step 2: run → FAIL** (`Config.Shards` / `ToShardedConfig` undefined).
`go test ./platform/storage/pg/ -run TestConfig_ToShardedConfig -count=1`

- [ ] **Step 3: implement in `config.go`** — add fields to `Config`:

```go
	// Shards, when non-empty, lists the writer DSN of each physical shard for
	// horizontal sharding (Tier-3, pg.ShardedPool). Empty (the default) ⇒ a
	// single shard built from DSN, byte-identical to the unsharded pool.
	Shards       []config.Secret `env:"PG_SHARDS" envSeparator:","`
	ReaderShards []config.Secret `env:"PG_READER_SHARDS" envSeparator:","`
```

Add the builder:

```go
// ToShardedConfig builds a ShardedConfig from this Config. With no Shards set it
// yields a single shard from DSN/ReaderDSN (M=1, identical to New(cfg)); with
// Shards set it yields one shard per listed DSN, carrying this Config's tuning
// in PerShard. PerShard.MigrateURL is preserved only for the single-shard case
// (NewSharded rejects it at M>1 — each shard migrates via its own DSN).
func (c Config) ToShardedConfig() ShardedConfig {
	per := c
	per.Shards = nil
	per.ReaderShards = nil
	if len(c.Shards) == 0 {
		return ShardedConfig{DSNs: []config.Secret{c.DSN}, ReaderDSNs: readerDSNs(c.ReaderDSN), PerShard: per}
	}
	per.MigrateURL = "" // M>1: each shard migrates via its own DSN
	return ShardedConfig{DSNs: c.Shards, ReaderDSNs: c.ReaderShards, PerShard: per}
}

// readerDSNs returns a single-element reader slice when a reader DSN is set,
// else nil (the shard inherits its writer).
func readerDSNs(r config.Secret) []config.Secret {
	if r == "" {
		return nil
	}
	return []config.Secret{r}
}
```

NB: `per := c` copies the env-slice fields too; setting `per.Shards = nil`/`per.ReaderShards = nil` avoids confusion (PerShard is a single-pool template). Confirm `ShardedConfig` field names (`DSNs`, `ReaderDSNs`, `PerShard`) from sub-project A.

- [ ] **Step 4: run → PASS.** `go test ./platform/storage/pg/ -run TestConfig_ToShardedConfig -count=1`
- [ ] **Step 5: commit** (`feat(pg): Config.ToShardedConfig + PG_SHARDS env`). Verify SHA.

---

### Task B2: `servicekit.Service` holds `*pg.ShardedPool`

The atomic core change — it will not compile until every internal user is updated, so this is ONE task. Read `platform/servicekit/service.go` (pool field :58, `Pool()` :282, build :190-208), and every internal consumer of `s.pool`/`Pool()` in `platform/servicekit/*.go`.

**Files:** Modify `platform/servicekit/service.go`, and every servicekit file referencing `s.pool` or `Pool()` (relay.go, cleanup.go, partition.go, audit.go, consumers.go, leader/health wiring). Test: the existing servicekit tests + `go build ./...`.

- [ ] **Step 1: change the field + construction** in `service.go`:
  - `pool *pg.Pool` → `shards *pg.ShardedPool`.
  - Build: `s.shards, err = pg.NewSharded(ctx, cfg.PG.ToShardedConfig())`; closer `s.shards.Close`; migrate `pg.MigrateSharded(ctx, s.shards, migrations, migrationsDir)` (gated by `MigrateOnStart`); health `AddReadiness("pg", s.shards.HealthCheck)`.
  - Replace the `Pool() *pg.Pool` accessor with `Shards() *pg.ShardedPool`. (Grep all call sites — services + gateway — they are updated in later tasks; for B2 only servicekit-internal callers must compile.)

- [ ] **Step 2: update servicekit-internal users of the pool.** For each that needs a single pool per shard, switch to per-shard wiring (Tasks B4 covers relay/cleaners/audit in depth — but B2 must at minimum COMPILE). The pragmatic approach: in B2, give `ShardedPool` callers a temporary bridge only where unavoidable, but PREFER to fold the real per-shard wiring in now if small. If a call site is large (relay), B2 may route it through `s.shards.Shards()[0]` with a `// TODO(B4): per-shard` ONLY as a compile bridge — but B4 MUST remove every such bridge (the self-review checks for `TODO(B4)`).

  Cleaner alternative (recommended): do B2 and B4 as one combined task if the compile coupling is tight — the controller may merge them. Keep inbox/consume (`consumers.go`) compiling by passing `s.shards` into the consumer (Task B3 implements the consume side; for B2 just thread the type).

- [ ] **Step 3: build + existing servicekit tests + `go build ./...`.**
`go build ./... && go test ./platform/servicekit/ -count=1 -p 1`
Expected: PASS. (Service-level tests boot with `M=1` from `PG_DSN` ⇒ identical behaviour.)

- [ ] **Step 4: commit** (`refactor(servicekit): Service holds *pg.ShardedPool (M=1 = single pool)`). Verify SHA.

> Because B2 cannot half-compile, the controller should treat B2 (+ the parts of B3/B4 needed to compile) as a single landing. Split the COMMITS by concern where possible, but it is acceptable for B2 to be one larger commit that keeps the tree green.

---

### Task B3: consumer shard-key middleware

**Files:** Modify `platform/messaging/consume/consume.go` (the `Consumer` struct `pool` field, `prepare`, `processRecord`, and the batch path). Read consume.go:130-310. Test: `platform/messaging/consume/*_test.go` (extend an existing consumer test, or add a 2-shard routing test).

- [ ] **Step 1: change `Consumer.pool`** from `*pg.Pool` to `*pg.ShardedPool` (it is built from `s.shards` in servicekit `AddConsumer`/`AddBatchConsumer` — update those constructors).

- [ ] **Step 2: set the shard key + resolve in `processRecord`** (and the batch equivalent):

```go
func (c *Consumer) processRecord(ctx context.Context, r kafka.Record) error {
	p := c.prepare(ctx, r)
	if p.skip {
		return nil
	}
	ctx = p.ctx
	// Route this record to its shard: the key is the aggregate id (order_id in
	// the choreography). Set it in ctx so the handler's repos resolve the same
	// shard, and resolve the pool for the inbox dedup tx.
	ctx = pg.WithShardKey(ctx, string(r.Key))
	shard := c.pool.Resolve(string(r.Key))
	...
	_, err := inbox.ProcessOnce(ctx, shard, c.group, p.msgID, func(ctx context.Context) error { ... })
	...
}
```

The `noInbox` test path also sets `ctx = pg.WithShardKey(ctx, string(r.Key))` (so handlers route). For the BATCH path (`BatchHandler`/`ProcessBatchOnce`): a batch may span shards. Group the batch's records BY SHARD (`c.pool.Resolve(string(r.Key))`) and run one `ProcessBatchOnce` per shard-group (each on its shard pool); set the shard key per record inside. If grouping the batch is too invasive for this task, fall back to per-record processing within the batch handler for the sharded (M>1) case and keep the single-group fast path for M=1 — document the choice. (M=1: one group = today's behaviour.)

- [ ] **Step 3: test** — a 2-shard consumer test: two records with keys hashing to different shards each land their inbox row + side effect on the resolved shard; M=1 unchanged. Run existing consume tests (must stay green).
`go test ./platform/messaging/consume/ -count=1 -p 1`

- [ ] **Step 4: commit** (`feat(consume): route each record to its shard by record key`). Verify SHA.

---

### Task B4: per-shard relay / cleaners / audit / partition

**Files:** Modify `platform/servicekit/relay.go`, `cleanup.go`, `partition.go`, `audit.go`. Read each (recon: relay.go:35-84, cleanup.go:39-91, partition.go:28-64, audit.go:15-25). Remove any `TODO(B4)` bridges from B2.

- [ ] **Step 1: relay per shard.** `AddOutboxRelay` currently builds N aggregate-shard relays on `s.pool`. Wrap it to iterate physical shards: `for i, p := range s.shards.Shards() { <build the existing relay(s) on p> }`. Each physical shard's outbox is independent; the per-shard relay keeps its own advisory-lock leader key (include the shard index in the lock name to avoid collision across shards — confirm the lock-key construction in `relay.go`/`outbox`). M=1 ⇒ one shard ⇒ identical to today.

- [ ] **Step 2: cleaners + partition per shard.** `registerInboxCleanup`, `AddAuditCleanup`, `AddOutboxPartitionMaintenance` run their work on each shard: wrap the per-tick function in `s.shards.ForEachShard(ctx, func(_ int, p *pg.Pool) error { <existing cleanup on p> })`. For audit cleanup, the admin pool must also be per-shard — if `PG_AUDIT_ADMIN_URL` is a single DSN it only covers M=1; for M>1 either require a per-shard admin DSN list or document that audit retention is M=1-only for now (flag it; do not silently clean only shard 0).

- [ ] **Step 3: audit writer + store per shard.** The async `BufferedAuditWriter` and `audit.PgStore` are per-shard (each shard owns its `audit_log` + chains). Build one writer per shard (`for each shard pool`), and `AddAuditWriter` each. The `audit.Audit`/`AsyncAudit` behavior resolves the writer/store for the record's shard — OR, simpler and matching the consumer seam: the audit store used inside a handler is the one for the ambient shard. Since the handler runs in the consumer's keyed tx on `shard`, the audit write (sync `audit.Audit` inside the same tx) lands on the same shard automatically IF the audit store routes via `sp.FromContext`. Decision: make `audit.PgStore` accept a `*pg.ShardedPool` (or be constructed per shard and selected by ctx key). Keep this minimal: per-shard `PgStore` keyed by ctx, mirroring the repos. Confirm the exact audit wiring and pick the lowest-churn option; document it.

- [ ] **Step 4: build + servicekit tests + M=1 service boot.**
`go build ./... && go test ./platform/servicekit/ -count=1 -p 1`

- [ ] **Step 5: commit** (`feat(servicekit): per-shard relay, cleaners, audit, partition maintenance`). Verify SHA.

- [ ] **Step 6: PHASE B CHECKPOINT — M=1 regression.** Run the orders + payments + notifications package tests and confirm green (M=1):
`go test ./examples/orders/... ./examples/payments/... ./examples/notifications/... -count=1 -p 1`
(Domain repos still take `*pg.Pool` until Phase C — if B forced a signature change that breaks them, that is Phase C work; keep the tree compiling by completing the minimal repo updates here or deferring the service wiring switch to C. The controller sequences so the tree is green at the checkpoint.)

---

## Phase C — domain repos + gateway

### Task C1: domain repos → `*pg.ShardedPool`

**Files:** `examples/orders/internal/domain/order/pg.go`, `examples/payments/internal/domain/payment/pg.go`, `examples/notifications/internal/domain/.../pg.go`, and each service's wiring (`orders.go:120`, `payments.go:107`, notifications) switching `svc.Pool()` → `svc.Shards()`.

- [ ] **Step 1** (per repo, apply the mechanical recipe): constructor `NewPgRepository(pool *pg.Pool)` → `NewPgRepository(sp *pg.ShardedPool)`; the `q(ctx)`/query helper `pg.FromContext(ctx, r.pool)` → `r.sp.FromContext(ctx)` (handle the returned error — propagate it). Update the service wiring call to pass `svc.Shards()`.
- [ ] **Step 2: run each service's tests** (`go test ./examples/orders/... -count=1 -p 1`, etc.). M=1 ⇒ identical. The command path runs inside the consumer's keyed tx, so `FromContext` returns the ambient tx — no shard key needed at the repo if a tx is open; but for safety the consumer set the key anyway.
- [ ] **Step 3: commit per service** (`refactor(orders): domain repo uses *pg.ShardedPool`, …). Verify SHAs.

### Task C2: gateway keyed writes (GetOrder, pending batcher, projection)

**Files:** `examples/gateway/internal/api/server.go` (GetOrder, pending insert, PendingBatcher wiring), `examples/gateway/internal/projection/projection.go`, the pending batcher source, gateway wiring `gateway.go:111`/`server.go:113-117`/`250`.

- [ ] **Step 1: GetOrder (keyed read).** Set `ctx = pg.WithShardKey(ctx, orderID)` then read via `sp.FromContextRead(ctx)`. Repo/handler takes `*pg.ShardedPool`.
- [ ] **Step 2: pending-row write.** Synchronous path: `WithShardKey(ctx, orderID)` + `sp.RunInTx`/`sp.FromContext`. Async `PendingBatcher`: it buffers rows across many orders that span shards — group the flush BY SHARD (`sp.Resolve(orderID)`), one multi-row insert per shard (mirror `BufferedAuditWriter.flush` grouping). M=1 ⇒ one group = today.
- [ ] **Step 3: projection.** `NewHandler(pool *pg.Pool)` → `*pg.ShardedPool`; writes via `sp.FromContext(ctx)`. The consumer middleware already set the shard key from the record key, so projection rows land on the order's shard.
- [ ] **Step 4: run gateway tests** (`go test ./examples/gateway/... -count=1 -p 1`). M=1 green.
- [ ] **Step 5: commit** (`refactor(gateway): keyed writes route via ShardedPool (GetOrder, pending, projection)`). Verify SHA.

### Task C3: gateway sharded `LIST /v1/orders` (scatter-gather merge)

**Files:** `examples/gateway/internal/app/list_orders.go` (handler + cursor encode/decode :55-80, query :97-170), `server.go:487-542`.

- [ ] **Step 1: failing test** — a 2-shard LIST test: insert orders across 2 shards, list with page size N, assert the returned page is globally ordered by `(created_at, id)` and matches what a single combined shard would return; cursor resume returns the next correct page with no dup/gap.
- [ ] **Step 2: implement scatter-gather.** Decode the opaque cursor into per-shard positions (one `(created_at, id)` cursor per shard, or empty). `sp.ForEachShard`: each shard runs the existing keyset query (`limit N after its cursor`). Merge: heap/k-way merge the M result sets by `(created_at, id)`, take the first N, encode the new cursor as each shard's last-emitted position (shards fully consumed keep their last cursor; unconsumed rows are re-queried next page — bounded by N). M=1 ⇒ one shard, the merge is a passthrough, cursor == today's cursor (keep backward-compatible encoding for M=1, or accept an opaque change — document).
- [ ] **Step 3: run → PASS** + M=1 list test unchanged.
- [ ] **Step 4: commit** (`feat(gateway): sharded LIST orders — scatter-gather k-way merge`). Verify SHA.

### Task C4: gateway sharded audit verify (fan-out)

**Files:** `examples/gateway/internal/api/server.go:449-482` (VerifyAudit), the audit store wiring.

- [ ] **Step 1: implement fan-out.** `VerifyAudit` runs `VerifyChain(ctx, since)` on every shard via `sp.ForEachShard` (the audit store is per-shard, or a sharded-aware verify). Aggregate: overall OK iff all shards OK; on a break, report the shard index + chain id + reason. M=1 ⇒ one shard = today's single result.
- [ ] **Step 2: test** — 2-shard verify: clean ⇒ all OK; tamper one shard's row ⇒ that shard reported, others OK.
- [ ] **Step 3: commit** (`feat(gateway): sharded audit verify — fan-out over shards`). Verify SHA.

- [ ] **PHASE C CHECKPOINT — M=1 e2e.** Run the full e2e choreography (`-short` off, M=1):
`go test ./examples/e2e/... -count=1 -p 1` (or the documented `just test-e2e`). Must be green — the byte-identity safety net.

---

## Phase D — infra + linearity proof

### Task D1: docker-compose sharding overlay

**Files:** new `docker-compose.shard.yml` (or extend `docker-compose.scale.yml`); read `docker-compose.yml:31-49` (postgres) + `docker-compose.scale.yml`.

- [ ] **Step 1:** add `postgres-shard-1` (and `-2`) Postgres services (distinct ports/volumes, same image/tuning as the base postgres). Set `PG_SHARDS` per app service to the list of shard DSNs (e.g. `postgres://app:app@postgres:5432/<db>?...,postgres://app:app@postgres-shard-1:5432/<db>?...`). Keep `PG_MIGRATE_URL` UNSET under sharding (each shard migrates via its own DSN). The default + scale stacks stay single-Postgres (`M=1`).
- [ ] **Step 2:** validate the compose file parses (`docker compose -f docker-compose.yml -f docker-compose.shard.yml config -q`). Do NOT require a full stack boot in the plan (heavy); the linearity proof (D2) uses testcontainers.
- [ ] **Step 3: commit** (`feat(compose): sharding overlay — multi-Postgres PG_SHARDS`). Verify SHA.

### Task D2: linearity proof (gated)

**Files:** a gated test, e.g. `examples/e2e/sharding_linearity_test.go` or `platform/storage/pg/sharded_linearity_test.go` (whichever can drive the order-create write path cleanly). Gate `TIER3_LINEARITY=1`.

- [ ] **Step 1:** build the write path against `M=1` then `M=2` (separate ShardedPools over 1 vs 2 testcontainers), drive the order-create-equivalent write (order row + outbox enqueue in one keyed tx) at concurrency across many distinct order_ids, measure sustained rate each.
- [ ] **Step 2:** log both rates + the ratio; assert `M=2` rate > `M=1` rate (materially — e.g. `> 1.3×`; the absolute K is machine-dependent, so assert improvement, not an absolute). This is the linearity proof.
- [ ] **Step 3:** run `TIER3_LINEARITY=1 go test ... -run Linearity -count=1 -p 1`, capture numbers.
- [ ] **Step 4: commit** (`test(tier3): linearity proof — M=2 write throughput > M=1`). Verify SHA.

### Task D3: docs

**Files:** `docs/operations.md` (§ Scaling), `.env.example`.

- [ ] **Step 1:** operations.md § Scaling — Tier-3: set `PG_SHARDS` (DSN list), the per-shard ceiling math (`5k = ceil(5000/K)`; the same code does `M×K`), `M=1`-default note, pointer to ADR-0019 and the compose overlay. `.env.example`: document `PG_SHARDS`/`PG_READER_SHARDS` (empty default = single pool).
- [ ] **Step 2: commit** (`docs(tier3): sharding operations guide + env`). Verify SHA.

---

## Self-review notes

- **Spec coverage:** ToShardedConfig/PG_SHARDS → B1; servicekit ShardedPool → B2; consumer shard-key → B3; per-shard relay/cleaner/audit/partition → B4; domain repos → C1; gateway keyed writes (GetOrder/pending/projection) → C2; sharded LIST merge → C3; audit verify fan-out → C4; compose → D1; linearity proof → D2; docs → D3.
- **M=1 byte-identity safety net:** checkpoints at end of B (orders/payments/notifications tests), end of C (e2e choreography), and the linearity test covers M=2. Every task notes the M=1 = today behaviour.
- **No-cross-shard-tx invariant:** the consumer resolves ONE shard pool per record and sets the matching ctx key; repos/audit resolve the same shard; the write + outbox + inbox + audit are one tx on that shard.
- **Known sharp edges flagged (not hidden):** (a) batch consumer must group by shard (B3) — M=1 keeps the single-group fast path; (b) audit retention admin pool is per-shard — if only a single `PG_AUDIT_ADMIN_URL` is configured, retention is M=1-only and must be flagged, never silently clean shard 0; (c) per-shard relay advisory-lock keys must include the shard index to avoid cross-shard leader collisions; (d) LIST cursor encoding changes to per-shard positions — keep M=1 compatible or document the opaque change.
- **B2/B3/B4 compile coupling:** B2 cannot half-compile; the controller may land B2 + the minimal B3/B4 threading together to keep the tree green, then split the remaining per-shard wiring into B3/B4 commits. Any `TODO(B4)` compile bridge introduced in B2 MUST be removed by B4 (grep for it).
- **Type consistency:** `Config.Shards`/`ToShardedConfig`/`ShardedConfig{DSNs,ReaderDSNs,PerShard}`/`Service.Shards()`/`sp.FromContext(ctx)(DBTX,error)`/`sp.RunInTx`/`pg.WithShardKey`/`sp.Resolve`/`sp.ForEachShard` — used identically across tasks and match sub-project A's shipped API.
