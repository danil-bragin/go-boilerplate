# ADR 0017 — Outbox relay sharding (ordered parallel publish)

**Status:** Accepted
**Date:** 2026-06-12

## Context

The outbox relay defaults to single-active leader mode (`OUTBOX_SINGLE_ACTIVE=
true`, ADR-0004): one instance per service holds an advisory lock and publishes,
which preserves per-aggregate event order across replicas but caps publish
throughput at one relay's drain loop. Under high write volume the relay becomes
the publish bottleneck even though Postgres and Kafka have headroom (the Tier-2
finding in `docs/operations.md` § Scaling).

The two existing escape hatches are both unsatisfactory at scale:

- `OUTBOX_SINGLE_ACTIVE=false` lets every instance publish concurrently (uses
  `FOR UPDATE SKIP LOCKED` for safety) — but **drops per-aggregate ordering**,
  which event-carried state transfer relies on.
- Vertically faster relay (bigger `OUTBOX_BATCH_SIZE`, shorter
  `OUTBOX_POLL_INTERVAL`) only goes so far on one publisher.

We want parallel publish **without** losing per-aggregate order.

## Decision

Add **opt-in relay sharding** (`OUTBOX_RELAY_SHARDS`, default 1). When `Shards =
N > 1`, `servicekit.AddOutboxRelay` wires a fleet of N relays instead of one:

- **Shard assignment by aggregate hash.** Each shard owns the rows whose
  `aggregate_id` hashes into it: `mod(hashtextextended(aggregate_id, 0), N) ==
  shard`. `hashtextextended` is the same hash family Postgres hash-partitioning
  uses, and `aggregate_id` is immutable, so a given aggregate maps to **exactly
  one shard for life**. The fetch is a sharded variant of the
  `FOR UPDATE SKIP LOCKED` query (`FetchUnpublishedShard`).
- **One leader per shard.** Each shard relay holds its OWN advisory lock
  (`outbox_relay:<schema>:shard:<i>`), so the N shards are leader-elected
  independently. Across a fleet of M instances the N shard-leaderships spread
  out — up to N instances publish at once.
- **Order is preserved.** All events of one aggregate are in one shard, and that
  shard has exactly one active publisher that drains them in `created_at` order.
  Different aggregates are independent, so cross-shard concurrency is safe.

Net effect: ~N× publish throughput while keeping the per-aggregate ordering
guarantee that `SINGLE_ACTIVE=false` sacrifices.

### Backward compatibility

`Shards = 1` (the default) is **byte-identical** to the previous behaviour: the
unsharded `FetchUnpublished` query and the bare `outbox_relay:<schema>` lock key
are used, gated on `cfg.SingleActive` exactly as before. The hot path is
untouched for anyone who does not opt in.

### What it does NOT change

- **Delivery semantics stay AT-LEAST-ONCE.** The same A/B/C three-phase publish
  and inbox dedup apply per shard; the fencing caveat (WithSingleActive) is
  unchanged — a lost lock still has a bounded dual-publish window, deduplicated
  downstream.
- **Cleaner and partition maintenance are shard-agnostic** and run once per
  instance / once as leader respectively, regardless of shard count.

## Consequences

- Going from 1 → N shards is a config change (`OUTBOX_RELAY_SHARDS=N`), no
  migration. Increasing N later re-buckets aggregates to different shards; since
  ordering is only required *per aggregate* and a re-bucket only changes *which*
  single leader owns an aggregate (never splits it across two at once for new
  rows), it is safe between deploys. Avoid changing N while a large backlog is
  draining if strict ordering during the transition matters.
- N should be ≤ the number of service instances to actually parallelise (extra
  shards beyond instance count still work — one instance leads several shards —
  but give no additional concurrency).
- Sharding lifts the **relay** bottleneck, not the single-Postgres-**writer**
  ceiling (ADR-0008). It is the highest-leverage Tier-2 step before sharding the
  database itself.
- Per-shard metrics: the relay instruments are global (counters sum across
  shards); shard identity is on the relay's error logs (`shard`/`shards`).
