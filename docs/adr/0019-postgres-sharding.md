# ADR 0019 — Horizontal Postgres sharding (`pg.ShardedPool`)

**Status:** Accepted
**Date:** 2026-06-13

## Context

Tier-2 removed the cross-aggregate serialization points: the outbox relay shards
by aggregate (ADR-0017) and the audit hash chain shards by actor (ADR-0018).
Read pressure is off the writer (reader/writer split) and the async-audit drain
was parallelized per chain. What remains is the **single Postgres writer
itself** (ADR-0008): every measurement converges on one instance's commit
capacity.

Measured locally (single Postgres, batched where applicable):
- ~40k trivial single-row inserts/s (the Eventual command path, A2),
- ~20k batched audit rows/s (parallel per-chain flush, A2),
- ~5–6k order-create transactions/s (the full effectively-once write path).

These are one-instance ceilings. A hash chain can be parallelized by adding more
chains; a single writer can only be scaled by adding more **writers**. The
linearity lever is horizontal sharding: `M` independent Postgres shards give
`~M×` write capacity for the same code — **5k = ceil(5000/K) shards; 50k by
changing one number.**

## Decision

Add an **opt-in** `pg.ShardedPool` to `platform/storage/pg`. The existing
`pg.Pool` and its `RunInTx`/`FromContext`/`Migrate` are NOT modified, so current
users see zero change.

- **`[]*pg.Pool` + `Router`.** A `ShardedPool` holds one tuned `pg.Pool` (today's
  writer/reader split, query tracer, sizing) per physical shard, plus a `Router`
  that maps an aggregate key to a physical shard.
- **Pinned FNV-1a 64-bit hash, NOT `maphash`.** `Router.Physical(key) =
  assign[fnv1a64(key) % 256]`. Determinism *across processes* is mandatory: the
  four example services are separate processes, and `maphash`'s per-process
  random seed would route the same `order_id` to different shards in the gateway
  and in `orders`, splitting an aggregate across shards. FNV-1a (stdlib
  `hash/fnv`, the same family as the audit `chain_id` hash) is seedless and
  identical everywhere.
- **256 fixed logical shards → static physical assignment.** Keys hash into 256
  logical shards; `assign[l] = l % M` maps logical → physical. The logical layer
  is forward-prep for a future resharding mechanism (keys never rehash; only the
  logical→physical assignment would move).
- **Routing key carried in context.** `WithShardKey(ctx, aggregateID)` →
  `sp.RunInTx`/`FromContext` resolve `Resolve(key)` and delegate to the existing
  single-pool functions (the ambient-tx machinery is reused unchanged). In the
  example choreography the key is `order_id` = the Kafka record key; consumer
  middleware injects it, the gateway injects it at the edge. A keyed operation
  with no shard key **fails closed** (error) — never a silent shard-0 write.
- **No cross-shard transaction.** An aggregate and everything atomic with it —
  its row, its outbox row, its inbox-dedup row, its audit chain — live on the
  same shard, resolved from the same key. Aggregate boundaries are chosen so no
  transaction spans shards, so effectively-once and per-aggregate order are
  preserved exactly.
- **Keyless operations fan out.** `ForEachShard` runs a function against every
  shard concurrently (joined error, never a silent partial result). Global
  reads (LIST), audit verification, and `MigrateSharded` use it; the gateway
  merges LIST results with the existing `(created_at, id)` keyset and per-shard
  cursors.
- **`M=1` is byte-identical.** One DSN ⇒ one pool, `assign[l]=0` ∀ l ⇒ every key
  resolves to that pool ⇒ behaviour identical to a plain `pg.Pool`. Adopting
  `ShardedPool` is opt-in.

## Consequences

- Going 1 → M is a deploy-time configuration of `M` DSNs; the same code does any
  `M`. The write path's single-instance ceiling becomes `~M×`.
- Effectively-once (ADR work), tamper-evidence (ADR-0018), and the A2 consistency
  policy are unchanged — they now apply per shard.
- Combined with ADR-0017 (relay sharding) and ADR-0018 (audit sharding), each
  physical shard runs its own relay leader / cleaner / audit chains, so no
  per-shard component reintroduces a global serialization point.

### Static `M` — deliberate, and the showcase

`M` is fixed at deploy. **Live resharding — changing `M` without downtime — is
explicitly deferred.** Growing `M` requires physically moving a logical shard's
data between nodes with a consistent cutover (dual-write + read-repair +
switch); that coordination is a project in itself. The 256-logical-shard
indirection exists precisely so that, when resharding is built, keys never
rehash — only the logical→physical assignment moves. Until then: pick `M` up
front, reshard offline if you must grow it. For a reference template this
boundary is a feature, not a gap: it states honestly what sharding does and does
not buy you.

### Known limitations / production hardening

None of these are blockers for the primitive; all are what a real production
sharding rollout must add on top:

- **Readiness = all-shards-up.** `HealthCheck` fails if any shard is down,
  because that shard's aggregates cannot be written. A production system may
  prefer per-aggregate availability (serve the shards that are up, 503 only the
  affected keys) — not built.
- **Assignment is a single source of truth.** Every service must share the same
  `M` and logical→physical map, or the same key routes differently in two
  services (the maphash failure mode, but via misconfiguration). A central,
  versioned assignment (config map / control plane) is the production answer.
- **Fan-out multiplies connections.** A process that talks to all `M` shards
  opens `M ×` pools. At large `M`, deploy instance-per-shard-group (Kafka
  partition assignment → one shard per instance) instead of one process per
  service holding every shard.
- **Schema changes must be expand-contract.** `MigrateSharded` runs goose per
  shard, non-atomically across the fleet, so a deploy is briefly mid-migration on
  some shards. Use additive (expand) then cleanup (contract) migrations, as the
  outbox-partitioning work already requires.

## References

- ADR-0008 (single-Postgres writer ceiling), ADR-0017 (outbox relay sharding),
  ADR-0018 (sharded audit hash chain).
- Spec: `docs/superpowers/specs/2026-06-13-tier3-shardedpool-primitive-design.md`.
- 5k-throughput design (Phase 2): `docs/superpowers/specs/2026-06-12-5k-order-create-throughput-design.md`.
- Follow-up (separate spec + plan): sub-projects B/C/D wire the example services,
  the gateway sharded reads, multi-Postgres compose, and the linearity load proof.
