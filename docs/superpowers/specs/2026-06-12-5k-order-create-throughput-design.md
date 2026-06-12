# Design — sustain 5,000 order-create/s with intact guarantees

**Date:** 2026-06-12
**Status:** Approved (brainstorm), pending implementation plan

## Goal

Drive `go-boilerplate` to a **stable 5,000 order-create/s** end-to-end, with
**every existing guarantee intact**:

- effectively-once processing (outbox + inbox),
- read-your-writes from the gateway projection (GET after POST),
- synchronous, atomic, tamper-evident audit (audit inside the command tx),
- per-aggregate event order.

This is an **event-choreography scale-out**, not a rewrite. The scale-out axis
already exists latently in Kafka partitioning by `aggregate_id`; the work is to
carry that axis down into the DB layer. Nothing existing is broken or
duplicated. If any change would touch RYW / exactly-once / order / audit, it
stops and is re-justified.

## Hard rules

1. No guarantee is sacrificed. A change that touches RYW, exactly-once, order, or
   audit atomicity halts for explicit re-justification.
2. Nothing already built is broken or duplicated. Audit first, then code.
3. Every change is validated through `platform/testkit/traffic` with the
   correctness ledger — zero regressions — and lands as its own commit with
   before/after throughput numbers.
4. Decisions are made by measurement, not assumption. Each phase re-measures
   before the next is justified.

## Phase 0 — Audit (complete; facts from the live code)

| # | Question | Finding |
|---|---|---|
| 1 | Projection consumer tx boundary | **Per-event.** `kafka/run.go` `EachRecord → h(ctx,r)` per record; `consume.Typed` wraps each in `inbox.ProcessOnce` (`consume.go:256`) → `pg.RunInTx` per record (`inbox.go:28`). Offset commit is per-poll (batched), but the DB write is one tx per event — the ~10–15k async tx/s at 5k order-create. **This is the Phase-1 lever.** |
| 2 | `platform/storage/pg` sharding | **None.** Reader/writer split + `RunInTx` + `RunAsLeader` only. No `ShardedPool`. Phase 2 builds it. |
| 3 | Outbox relay batch + leader | **Batch already.** `FetchUnpublished LIMIT BatchSize` + `PublishBatch → producer.ProduceBatch` (`publisher.go:158`, real franz-go batch). Single-active per service by default; **per-shard sharding already landed** (ADR-0017, `OUTBOX_RELAY_SHARDS`). Phase 3 is mostly done; the remainder is binding shard→pool once `ShardedPool` exists. |
| 4 | `orders.events/commands` partition key | **= `aggregate_id` (order_id).** `publisher.go:131 Key: []byte(msg.AggregateID)`. Kafka partitioning aligns with the aggregate — the latent scale-out axis is confirmed. |
| 5 | `cmd/projection` separable + scalable | **Yes.** Consumer-only binary; `GATEWAY_EMBEDDED_PROJECTION=false` splits it; shares the `gateway-projection` consumer group; scales to the partition count. |

**Audit conclusion:** the pipeline is already fast (measured p99 ≤ 5 ms) and the
cross-aggregate serialization points are gone (Tier-2: sharded relay ADR-0017 +
sharded audit ADR-0018). The remaining cost at 5k is **commit volume on the
projection writer** and **single-writer capacity**. The hottest writer is the
gateway projection (~2–3 projection tx per order on top of the per-request
pending insert).

## Phase 1 — Batch-apply in the projection consumer (main lever)

Add a **batch mode** to `platform/messaging/consume`. The existing per-record
`ProcessOnce` path is **NOT removed** — it becomes the fallback. Goal: collapse
the per-event projection commits into one commit per batch per partition,
cutting the ~10–15k async tx/s by a large factor without latency growth (this
path is already asynchronous). The REST pending insert stays per-request (that
is RYW and is untouched).

### Invariants (unbreakable)

1. **Batch unit = records of ONE partition from one poll.** This also pre-aligns
   the batch with a single pool under the future `ShardedPool`.
2. **Happy path:** inbox-insert + projection-write of all records run in ONE
   batch transaction, in partition offset order (preserving per-key order).
   `OnCommitted` hooks fire per-record, in order, after the commit.
3. **Strict all-or-nothing:** on rollback, NO side effect persists — including
   the inbox rows. The fallback therefore sees every record as unprocessed →
   no double-apply, by construction.
4. **Fallback on any batch-tx error:** reuse the existing per-record
   `ProcessOnce`, one record at a time, in the same offset order, **not**
   parallelized. A poison record flows into the existing tiered-retry / DLT
   path. DLT semantics are unchanged.
5. **Pre-tx filter:** decode/validate every record BEFORE opening the batch tx;
   malformed records go straight to the DLT, so only decodable records enter the
   tx. Runtime errors (constraint / serialization / deadlock) are still caught
   by the fallback in (4) — pre-validate does not replace it.
6. **Offset commit stays per-poll and ONLY after full poll success** (batch
   committed OR fallback finished, including poison→DLT). A crash before that
   redelivers the whole poll → inbox dedup makes it safe. The offset never
   advances over incomplete processing.

### Observability

`batch_fallback_total` counter + poison rate. An alert on a rising fallback rate
is the trigger to reconsider sub-batch bisection (deferred — bisection adds new
correctness surface and only helps under frequent poison + large batches; not
done without this metric proving the need).

### Validation

`testkit/traffic` with **poison-record injection** into the stream. Prove:
(a) happy-path throughput exceeds the per-event baseline;
(b) a poison record is isolated to the DLT;
(c) the rest of the batch still applies;
(d) per-key order and exactly-once hold per the correctness ledger;
(e) a crash mid-poll leaves the offset un-advanced and produces no duplicates on
restart.

### Decision point

Phase 1 plus the existing DB-per-service write spread may already close 5k. We
re-measure after Phase 1 and only proceed to Phase 2 if a writer is still
saturating. Bisection (the rejected poison-handling option) stays deferred
behind the fallback-rate metric.

## Phase 2 — `ShardedPool` in `platform/storage/pg` (headroom + linearity)

Only if Phase-1 measurement shows a saturating writer. Logical shards (e.g. 256)
→ M physical pools, routing by `hash(aggregate_id)`, in the same paradigm as the
existing tx-runner + reader/writer split. The Kafka-partition → pool mapping is
direct. Deploy `cmd/projection` as instance-per-partition-group, each writing
its own pool. N is config; **no rehash of aggregates when N grows** (logical
shards are stable; only the logical→physical assignment moves). Per-shard ACID
and per-aggregate order are preserved because a key maps to exactly one shard.

## Phase 3 — Relay throughput (mostly done)

Guarantee batch-read + `SendBatch`-publish (already true). Relay sharding per
pool (single-active leader per shard) already exists (ADR-0017); the remaining
work is to bind each relay shard to its `ShardedPool` pool once Phase 2 lands, so
one leader never becomes the ceiling at 5k.

## Phase 4 — Proof + contract

Run `testkit/traffic` at 5,000 order-create/s, find the first saturating writer,
and fix only that. Measure the per-shard ceiling ~K/s and record in
`docs/operations.md`: **5k = ceil(5000/K) shards; the same code does 50k by
changing the number.** Attach a reproducible run.

## Order of work

`0 (done) → report → 1 → re-measure → (2/3 only if measurement demands) → 4`.
Each phase is a separate commit with before/after numbers. Possibly Phase 1 +
the current DB-per-service spread already closes 5k, making Phase 2 a
headroom/linearity proof rather than a necessity — decided by measurement.

## Out of scope / explicitly deferred

- Sub-batch bisection poison handling (behind the fallback-rate metric).
- Any guarantee relaxation (async audit, dropped exactly-once, weakened RYW).
- A rewrite away from event choreography.
