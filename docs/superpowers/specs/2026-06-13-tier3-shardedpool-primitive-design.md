# Design — Tier-3 sub-project A: `pg.ShardedPool` platform primitive

**Date:** 2026-06-13
**Status:** Approved (brainstorm), pending implementation plan
**Sub-project:** A of Tier-3 (the platform primitive). B/C/D (examples wiring, gateway reads, load proof) are a separate spec + a single phased plan that consumes this primitive.

## Goal

Add horizontal Postgres sharding to `platform/storage/pg` as a new, opt-in
primitive — `pg.ShardedPool` — that routes each operation to one physical shard
by a stable hash of an aggregate key carried in the context. This is the
headroom/linearity lever from the 5k-throughput spec (Phase 2): once a single
Postgres writer saturates (~40k trivial inserts/s, ~20k batched audit rows/s,
~5–6k order-tx/s measured), `M` shards give `~M×` capacity for the same code —
**5k = ceil(5000/K) shards; 50k by changing one number.**

This sub-project ships ONLY the platform primitive (lives in
`platform/storage/pg/`, the actual boilerplate product). The existing `pg.Pool`
is untouched, so current users see zero change; a `ShardedPool` with `M=1`
behaves byte-identically to today's single pool.

## Hard rules (preserved by every part)

1. **No cross-shard transaction.** An aggregate and everything that must be
   atomic with it (its row, its outbox row, its inbox-dedup row, its audit chain)
   live on the SAME shard, resolved from the SAME context key. Aggregate
   boundaries are chosen so no transaction ever spans shards.
2. **Effectively-once is untouched.** The outbox insert and the aggregate write
   are one transaction on one shard (both resolve to the same pool) — delivery
   guarantees are unchanged.
3. **Per-aggregate order preserved.** A key maps to exactly one shard for life;
   the Kafka partition (keyed by the same aggregate id) → shard mapping is direct.
4. **`M=1` is byte-identical** to the current single-pool behaviour. Adopting
   `ShardedPool` is opt-in; `pg.Pool` and its `RunInTx`/`FromContext` are not
   modified.
5. **Deterministic, cross-process-stable routing.** The hash is pinned
   (`hash/fnv` FNV-1a 64-bit) — NOT `maphash` (whose per-process random seed would
   send the same key to different shards in different services and break the
   choreography on the spot).

## Routing model

```
shardKey (aggregate_id, e.g. order_id)
   │  fnv1a64(key)
   ▼
logical = hash % LogicalShards        // LogicalShards = 256, fixed
   │  assign[logical]                 // static logical→physical map
   ▼
physical ∈ [0, M)                     // M = number of configured DSNs
   ▼
shards[physical] *pg.Pool             // today's writer+reader pool, per shard
```

- **256 logical shards, fixed.** The logical layer is forward-prep for future
  resharding: keys never rehash; only the logical→physical *assignment* would
  move. In this static build the assignment is computed once from `M`
  (`assign[l] = l % M` by default, or an explicit map) and `M` is fixed for the
  life of the deployment.
- **Changing `M` live is resharding — explicitly deferred** (it requires
  physically moving a logical shard's data between nodes, with cutover). ADR-0019
  records this as a deliberate boundary, not a gap. Today: pick `M` at deploy,
  reshard offline if you must grow it.
- `M=1` ⇒ one physical pool, `assign[l]=0` ∀ l ⇒ every key resolves to the one
  pool ⇒ identical to a plain `pg.Pool`.

## Components (`platform/storage/pg/`)

All new files; `pool.go`/`tx.go`/`config.go` for the single pool are NOT changed.

### 1. `sharded_router.go` — `Router`
- `fnv1a64(key string) uint64` (pinned; stdlib `hash/fnv`).
- `Router{ logicalShards int; assign []int /* len=logicalShards, value ∈ [0,M) */; m int }`.
- `NewRouter(m int, opts...) *Router` — default `logicalShards=256`, `assign[l]=l%m`.
- `(*Router) Physical(key string) int` → `assign[fnv1a64(key) % logicalShards]`.
- Pure, no I/O — unit-testable, and the cross-process-determinism guard test
  asserts a table of known `key → physical` for a fixed `M` (so a future hash
  change is caught).

### 2. `sharded_pool.go` — `ShardedPool`
- `ShardedPool{ shards []*Pool; router *Router }`.
- `NewSharded(ctx, ShardedConfig) (*ShardedPool, error)` — builds one `*Pool`
  per DSN (reusing the existing `New` + all tuning, reader/writer split, query
  tracer per shard) and a `Router` over `len(DSNs)`.
- `(*ShardedPool) Resolve(key string) *Pool` → `shards[router.Physical(key)]`.
- `(*ShardedPool) Shards() []*Pool` and `Len() int`.
- `(*ShardedPool) ForEachShard(ctx, fn func(idx int, p *Pool) error) error` —
  runs `fn` against every physical shard CONCURRENTLY (errgroup-style), returns a
  joined error naming the failing shard(s). Used for keyless ops (fan-out) and
  for sharded migrations. Partial failure → error, never a silent partial result.
- `(*ShardedPool) HealthCheck(ctx) error` — healthy iff ALL shards healthy
  (readiness = all-up; see ADR-0019 known-limitations for why, and the
  per-aggregate-availability alternative).
- `(*ShardedPool) Close(ctx) error` — closes all shards, joins errors.

### 3. `sharded_tx.go` — context seam (mirrors `tx.go`, keyed)
- `WithShardKey(ctx, key string) context.Context` + `shardKeyFrom(ctx) (string, bool)`.
- `(*ShardedPool) RunInTx(ctx, fn func(ctx) error) error` — resolves the pool
  from the ctx shard key (error, fail-closed, if absent) and delegates to the
  existing `pg.RunInTx(ctx, resolvedPool, fn)`. The ambient-tx ctx key is the
  SAME one `pg.RunInTx` already uses, so nested `FromContext` calls inside `fn`
  see the transaction regardless of which pool resolved it.
- `(*ShardedPool) FromContext(ctx) DBTX` → `pg.FromContext(ctx, sp.Resolve(key))`
  (returns the ambient tx if one is open, else the resolved shard's pool). Errors
  fail-closed when no shard key is in ctx.
- `(*ShardedPool) FromContextRead(ctx) DBTX` → reader variant.
- A keyed operation with no shard key in ctx is a programming error → returns a
  clear `pg: no shard key in context` error (never silently routes to shard 0).

### 4. `sharded_migrate.go` — `MigrateSharded`
- `MigrateSharded(ctx, sp *ShardedPool, fsys, dir) error` — runs the existing
  `Migrate` against every physical shard via `ForEachShard`. Each shard migrates
  independently and atomically (the existing advisory-lock-per-connection
  behaviour holds per shard). `M=1` ⇒ exactly one `Migrate`, identical to today.

### 5. `ShardedConfig` (in `config.go`, additive — does NOT change `Config`)
- `ShardedConfig{ DSNs []config.Secret; ReaderDSNs []config.Secret /* optional, per-shard */; PerShard Config /* tuning applied to each */ }`.
- Env shape (documented; examples wire it in B/C/D): `PG_SHARDS` =
  comma/space-separated DSN list (1..M); per-shard reader DSNs optional. `M=1` is
  the default and == a single `PG_DSN`.

## Data flow (primitive level)

```
write:  ctx = WithShardKey(ctx, order_id)
        sp.RunInTx(ctx, fn) → P = sp.Resolve(order_id) → pg.RunInTx(ctx, P, fn)
                            → inside fn: sp.FromContext(ctx) == the tx on P
keyless: sp.ForEachShard(ctx, fn) → fn over all M pools concurrently → caller merges
```

The consumer middleware (sub-project B) calls `WithShardKey` from the Kafka
record key before the handler; the gateway (sub-project C) calls it explicitly
from the order id. This spec provides only the seam.

## Error handling

- Missing shard key on a keyed op → fail-closed error (caught in tests).
- One shard down → that aggregate's writes fail (cannot be relaxed without
  breaking durability); `HealthCheck` reports unhealthy → readiness 503.
- `ForEachShard` partial failure → joined error identifying the shard; no partial
  success masquerading as complete.
- Migration failure on any shard → `MigrateSharded` returns that shard's error;
  shards that succeeded stay migrated (re-run is idempotent via goose).

## Testing (this sub-project)

- `Router`: determinism table (known `key → physical` for `M∈{1,3,4}`) — the
  cross-process guard; even distribution sanity over many keys; `M=1` ⇒ all keys
  → 0.
- `ShardedPool.Resolve`: a key always resolves to the same pool; distinct keys
  spread across shards.
- `M=1` identity: a `ShardedPool` over one DSN behaves exactly like the
  underlying `pg.Pool` for `RunInTx`/`FromContext` (same rows, same tx semantics)
  — the byte-identity guarantee.
- `RunInTx` (integration, 2-shard testcontainers): a write under
  `WithShardKey(k)` lands on `Resolve(k)` and nowhere else; the ambient tx is
  visible to `FromContext` inside `fn`; rollback rolls back only that shard.
- `ForEachShard`: runs on all shards; partial failure → joined error; concurrency
  (no data race, `-race`).
- `MigrateSharded`: schema present on every shard; `M=1` identical to `Migrate`.
- Fail-closed: keyed op without a shard key → error, not a shard-0 write.

## ADR-0019 (written in this sub-project)

`docs/adr/0019-postgres-sharding.md` records the Tier-3 design and — prominently
— its deliberate boundaries (the showcase value for a reference template):
- **Static `M`**: pinned at deploy; 256 logical shards are headroom for a future
  resharding mechanism; **live resharding (changing `M` without downtime) is
  explicitly deferred** and why (data movement + cutover coordination).
- **`M=1` = byte-identity**; sharding is opt-in; `pg.Pool` unchanged.
- **Pinned FNV-1a 64-bit** hash (not `maphash`) for cross-process determinism.
- **Known limitations / production hardening** (one section, honest):
  readiness=all-shards-up (vs per-aggregate availability); the logical→physical
  assignment as a single source of truth that all services must share; fan-out
  fans out connection usage (M× pools per process unless deployed
  instance-per-shard-group); schema changes must be expand-contract because they
  run per shard non-atomically across the fleet. None are blockers for the
  primitive; all are what a real production sharding rollout must add.

## Out of scope (this sub-project — belongs to B/C/D or deferred)

- Consumer record-key → shard-key middleware; servicekit per-shard
  relay/cleaner/audit wiring (sub-project B).
- Gateway edge routing, sharded `LIST` k-way merge, sharded audit verify
  (sub-project C). The merge uses the existing `(created_at, id)` keyset with
  per-shard cursors — a gateway concern, not a primitive one.
- Wiring all four example services, `docker-compose` multi-Postgres, the
  linearity load proof (sub-project D).
- Live resharding / rebalancing (deferred; ADR-0019).
- Cross-shard transactions, global secondary indexes, distributed joins (out of
  scope by design — aggregate boundaries avoid them).
