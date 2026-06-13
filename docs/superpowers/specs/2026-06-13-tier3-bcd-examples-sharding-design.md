# Design — Tier-3 sub-projects B/C/D: wire `pg.ShardedPool` into the examples

**Date:** 2026-06-13
**Status:** Approved (brainstorm — Tier-3 design), pending implementation plan
**Depends on:** sub-project A (`pg.ShardedPool` primitive, shipped). Spec A:
`docs/superpowers/specs/2026-06-13-tier3-shardedpool-primitive-design.md`.

## Goal

Wire the shipped `pg.ShardedPool` primitive through the `servicekit` harness and
all four example services so the whole choreography runs sharded, then prove
write-path linearity (per-shard ceiling `K` → `M` shards ≈ `M×K`). `M=1` (the
default, `PG_SHARDS` unset) MUST keep every existing test — unit, integration,
e2e choreography, traffic, chaos — byte-identically green; that is the safety
net. `M≥2` is the new capability and the linearity proof.

## Confirmed facts (from a full recon of the examples)

- **Every Kafka record key in the choreography is `order_id`** (gateway
  `CreateOrderCommand` key; orders `OrderCreated`/`OrderPaymentTimedOut`,
  aggregate_id = orderID; payments `PaymentProcessed`/`PaymentFailed`,
  aggregate_id = `p.OrderID`, NOT payment_id; the outbox publisher keys by
  `aggregate_id`). So a single uniform shard key — `order_id` — co-locates an
  order's entire lifecycle on one shard within each service's own sharded DB.
- `servicekit.Service` holds `pool *pg.Pool` (service.go:58) + `Pool()`
  accessor; built by `pg.New(ctx, cfg.PG)` (service.go:192). All services and
  the gateway read pools through `svc.Pool()`.
- Domain repos take `*pg.Pool` and call `pg.FromContext(ctx, pool)`.
- Consumer routing seam: `consume.Consumer.prepare()` has the `kafka.Record`
  (with `.Key`); `inbox.ProcessOnce(ctx, pool, consumer, msgID, fn)` opens the
  per-record tx.

## Integration strategy

**`servicekit` adopts `*pg.ShardedPool` uniformly.** The harness builds a
`ShardedPool` always: `M=1` from `PG_DSN` when `PG_SHARDS` is empty (byte-
identical to today), `M≥1` from `PG_SHARDS` (a DSN list). `Service.pool` becomes
`*pg.ShardedPool`; `Pool()` is replaced by `Shards() *pg.ShardedPool`. This is
the "examples are templates: uniformity beats YAGNI" choice — every service is
sharding-ready; `M=1` costs nothing.

Routing is invisible to handler code: the consumer middleware sets the shard key
from the record key once, and domain repos call `sp.FromContext(ctx)` /
`sp.RunInTx(ctx, fn)`. Within a request the key is stable, so the write, its
outbox row, its inbox-dedup row, and its audit chain all resolve to one shard —
no cross-shard tx (the ADR-0019 invariant).

## Sub-project B — `servicekit` core + consumer routing

1. **`pg.Config` → ShardedConfig.** Add `PG_SHARDS` (DSN list; empty ⇒ use the
   single `PG_DSN`) and optional `PG_READER_SHARDS`. A `Config.ToShardedConfig()`
   builds `pg.ShardedConfig` (1 shard from `DSN`/`ReaderDSN` when `PG_SHARDS`
   empty; else one per listed DSN, with `PerShard` carrying the tuning and — per
   ADR-0019/the A-primitive guard — `MigrateURL` only allowed at `M=1`).
2. **`servicekit.Service` holds `*pg.ShardedPool`.** `New` builds it via
   `pg.NewSharded`, migrates via `pg.MigrateSharded`, health-checks via
   `sp.HealthCheck`, closes via `sp.Close`. `Pool()` → `Shards() *pg.ShardedPool`.
   Every internal use (`AddOutboxRelay`, cleaners, partition maint, audit) moves
   to per-shard wiring.
3. **Consumer middleware.** In `consume.Consumer.prepare()` (or `processRecord`),
   set `ctx = pg.WithShardKey(ctx, string(r.Key))` before the handler, and
   resolve the shard pool from the key to pass into `inbox.ProcessOnce` (the
   inbox stays `*pg.Pool`; the consumer hands it the resolved shard pool). The
   handler's repos then route to the same shard via `sp.FromContext(ctx)`. A
   record with an empty key fails closed (it must be keyed — all choreography
   events are).
4. **Per-shard relay / cleaners / partition / audit.** `AddOutboxRelay` spawns
   the relay (with its existing aggregate-sharding) once **per physical shard**
   (each shard's outbox is independent). Inbox/audit retention cleaners and the
   outbox partition manager iterate `sp.ForEachShard`. The async audit writer is
   built per shard (each shard has its own `audit_log` + chains).

## Sub-project C — domain repos + gateway

5. **Domain repos** (orders, payments, notifications): constructor takes
   `*pg.ShardedPool`; query methods call `sp.FromContext(ctx)` /
   `sp.FromContextRead(ctx)` instead of `pg.FromContext(ctx, pool)`. The command
   path already runs inside the consumer's keyed tx, so routing is ambient.
6. **Gateway keyed writes.** `GetOrder` (keyed by order id) sets the shard key
   and reads via `sp.FromContextRead`. The pending-row write sets the shard key
   from the order id; the async `PendingBatcher` (`GATEWAY_PENDING_ASYNC`) groups
   buffered rows **by shard** and does one multi-row insert per shard (mirrors
   the audit writer's flush-by-chain). The projection write path routes via the
   consumer middleware's shard key (no change beyond switching to
   `sp.FromContext`).
7. **Gateway sharded `LIST /v1/orders`.** Scatter-gather: query each shard for a
   page (limit N after the per-shard cursor position), k-way merge by the
   existing `(created_at, id)` keyset (id globally unique — UUIDv5 — so the
   tiebreak is stable across pages). The opaque cursor encodes per-shard
   positions. `M=1` ⇒ one shard, no merge — identical to today.
8. **Gateway sharded audit verify.** `GET /v1/audit/verify` runs
   `VerifyChain` on every shard via `sp.ForEachShard` and aggregates: OK only if
   all shards verify; a break reports the shard + chain + reason.

## Sub-project D — infra + linearity proof

9. **`docker-compose`.** Add shard Postgres instances (e.g. `postgres-shard-1`,
   `postgres-shard-2`) to a sharding overlay (`docker-compose.scale.yml` or a new
   `docker-compose.shard.yml`); set `PG_SHARDS` for each service. The default
   stack stays single-Postgres (`M=1`).
10. **Linearity proof.** A gated test/bench (`A2_CAP`-style, e.g.
    `TIER3_LINEARITY=1`) measures the order-create write path against `M=1` vs
    `M=2` shards and asserts `M=2` throughput materially exceeds `M=1` (logged;
    the absolute K is machine-dependent). Reuse `testkit/traffic` if practical;
    otherwise a focused 2-shard write loop.
11. **Docs.** `docs/operations.md` § Scaling — Tier-3: how to set `PG_SHARDS`,
    the per-shard ceiling math (`5k = ceil(5000/K)`), and a pointer to ADR-0019.

## Guarantees / invariants (preserved)

- Effectively-once: order write + outbox enqueue stay one tx on one shard.
- Per-order ordering: order_id → one shard for life; Kafka partition (keyed by
  order_id) → shard mapping is direct.
- `M=1` byte-identity: all existing e2e/traffic/chaos green unchanged.
- Tamper-evidence: per-shard audit chains; verify fans out.
- No cross-shard transaction anywhere.

## Testing

- **`M=1` regression (the safety net):** the full existing suite — unit,
  integration, e2e choreography, traffic, chaos — green unchanged after every
  phase. This is the headline: sharding-ready code, zero behaviour change at
  `M=1`.
- **`M≥2` isolation:** an order's rows land only on `hash(order_id)` shard;
  effectively-once holds across 2 shards; per-shard audit verifies.
- **Sharded LIST:** 2-shard merge returns the same globally-ordered page a
  single shard would, with correct cursor resume.
- **Linearity proof (D):** `M=2` write throughput > `M=1` (gated, logged).

## Out of scope / deferred

- Live resharding / rebalancing (ADR-0019).
- Cross-shard transactions / global secondary indexes / distributed joins.
- Sharding the `RecordProductView` A2 demo (it has no outbox; leave single — or
  route opportunistically if trivial, but not required).
- Per-aggregate availability (readiness stays all-shards-up; ADR-0019).
