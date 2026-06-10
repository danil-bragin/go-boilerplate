# ADR 0004 — Reliable messaging: outbox + inbox over Kafka EOS

**Status:** Accepted (amended 2026-06-10)  
**Date:** 2026-06-08

## Context

The boilerplate needs a strategy for reliable, effectively-once event delivery between services over Kafka. Two main approaches: Kafka Exactly-Once Semantics (EOS) via `GroupTransactSession` + idempotent producer, or the transactional outbox (producer side) + inbox dedup table (consumer side) pattern. EOS is Kafka-native but requires careful handling of rebalance-triggered transaction aborts, forces co-location of the consume and produce calls in a single Kafka transaction, and does not protect against failures that occur after the Kafka commit but before the DB write.

## Decision

Use **transactional outbox + inbox** for v1:

- **Outbox:** command handlers enqueue domain event rows in the same DB transaction as the state change. A polling relay (`platform/messaging/outbox.Relay`) publishes unpublished rows to Kafka using `FOR UPDATE SKIP LOCKED`, then marks them published — AT-LEAST-ONCE delivery to Kafka. The relay stamps each record with a stable `message-id` header (the outbox row UUID).
- **Inbox:** consumers call `inbox.ProcessOnce(consumer, messageID, fn)` which inserts a dedup row and runs the business logic in the same DB transaction — giving atomic dedup + effect, effectively-once per `(consumer, messageID)`.

Kafka EOS (`GroupTransactSession`) is reserved for money-grade atomic consume→produce flows and documented as a future enhancement.

## Consequences

- The pattern works with any Kafka delivery mode and survives broker restarts, consumer rebalances, and network partitions.
- Complexity lives in the DB (two small tables per service) rather than in Kafka transaction coordination.
- The polling relay introduces a small additional latency (default 1 s poll interval, configurable via `OUTBOX_POLL_INTERVAL`; the example services run 200 ms). The relay drains the table until empty each tick, so throughput is not capped at `BatchSize × poll rate`.
- Services must have an `outbox` table and an `inbox` table in their migrations (the `payments` service is the canonical template).

## Amendment (2026-06-10)

**Relay scale-out path.** The polling relay is stage one of a deliberate ladder; climb only when measurements demand it:

1. **Polling relay** (current): simplest, no broker-side or DB-side extras; latency floor = poll interval.
2. **LISTEN/NOTIFY wake-up** (middle tier): keep the same relay, add a Postgres `NOTIFY` on outbox insert and a `LISTEN`ing relay that polls on notification instead of on a timer — sub-poll-interval latency with ~zero new infrastructure. The poll loop stays as the fallback (NOTIFY is best-effort, lost on reconnect).
3. **Logical replication / CDC (Debezium)** (last tier): a separate connector fleet tails the WAL and publishes outbox rows. Lowest latency and zero query load on the table, but brings Kafka Connect (or equivalent) operations, slot management, and replica-identity concerns. Adopt only when tier 2's write amplification or latency is measurably insufficient.

**Single-active relay ordering note.** Multiple concurrent relays preserve at-least-once delivery but can reorder events *across batches*, breaking per-aggregate order on the wire. The relay therefore runs in single-active mode by default (`OUTBOX_SINGLE_ACTIVE=true`): one leader per service schema holds a Postgres advisory lock; standbys poll for takeover. Run multiple active relays only when every consumer of the affected topics is reorder-safe.

**Platform is a TEMPLATE, not a library (recorded decision).** `platform/` is consumed by copying the repository (forks diverge), not by importing it as a versioned Go module. Consequence: fixes do not propagate to forks automatically — forks cherry-pick from upstream. This was chosen deliberately: a shared library would force lowest-common-denominator APIs, semver ceremony on every behavioral fix, and would prohibit the local surgery (deleting unused packages, hard-coding org policy) that makes a boilerplate worth forking. Revisit only if 3+ internal forks are actively cherry-picking the same fixes.
