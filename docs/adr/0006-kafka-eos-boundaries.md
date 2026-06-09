# ADR 0006 — Kafka EOS boundaries: when to use each once-semantics tool

**Status:** Accepted  
**Date:** 2026-06-10

## Context

The boilerplate ships three tools that each address a different slice of the
"once-semantics" problem:

| Tool | Transport | Atomicity boundary | Delivery |
|------|-----------|-------------------|----------|
| Transactional outbox (`platform/messaging/outbox`) | DB → Kafka | DB transaction (state change + enqueue in one commit) | At-least-once to Kafka |
| Inbox dedup (`platform/messaging/inbox`) | Kafka → DB | DB transaction (dedup row + effect in one commit) | Effectively-once per `(consumer, message-id)` |
| `TransactConsumer` (`platform/messaging/kafka`) | Kafka → Kafka | Kafka transaction (offsets + output records in one commit) | Exactly-once for pure kafka→kafka |

Each tool covers exactly one hop. Combining them lets a full pipeline achieve
effectively-once end-to-end even though no single tool spans the whole chain.

A Kafka transaction has a hard boundary: it cannot atomically commit both a
Kafka offset/record set and a database row. Any attempt to extend a Kafka
transaction across a database write sacrifices either the Kafka guarantee (if
the DB write fails after Kafka commit) or the DB guarantee (if the Kafka commit
fails after DB write).

## Decision

**Use each tool only within its declared boundary:**

1. **Outbox → Kafka paths** (service writes state to Postgres): use the
   transactional outbox. The relay publishes the event to Kafka at-least-once;
   downstream consumers use inbox dedup. This is the pattern for all four
   example services in this repo (`orders`, `payments`, `notifications`,
   `inventory`) because they all touch Postgres.

2. **Kafka → Kafka paths** (pure stream-processing with no DB write): use
   `TransactConsumer` as an opt-in capability. Input offsets and output records
   commit atomically inside a single Kafka transaction. Aborted batches are
   invisible to read-committed consumers (the default for all `NewConsumer` and
   `retry.NewConsumer` instances as of this ADR).

3. **Kafka → DB paths**: use inbox dedup (`inbox.ProcessOnce`). The at-least-once
   delivery from upstream is deduplicated on the consumer side inside the same
   DB transaction as the business effect.

All three patterns coexist in a single deployment; choose based on the hop, not
on a global preference.

## Consequences

- **Transactional overhead**: each `TransactConsumer` batch incurs one extra
  broker round-trip (EndTransaction) and a heartbeat force. Latency per batch
  increases by roughly one broker RTT (~1–5 ms on a co-located cluster). For
  throughput-sensitive pipelines this is acceptable; for latency-sensitive
  ones, batch size should be tuned to amortise the overhead.

- **Read-committed is now the default**: `kafka.NewConsumer` and
  `retry.NewConsumer` both set `FetchIsolationLevel(ReadCommitted())`. This has
  no observable effect on topics that are written non-transactionally, but it
  means any transactional producer's aborted records are automatically invisible
  without any consumer-side configuration.

- **Downstream consumers must use read-committed** when consuming output topics
  of a `TransactConsumer`. Because our default is now read-committed, this
  requirement is satisfied automatically for all consumers built with this
  boilerplate.

- **Services that touch Postgres stay on outbox**. The four example services are
  deliberately NOT migrated to `TransactConsumer` because they write to Postgres.
  Mixing a Kafka transaction with a Postgres write in the same handler would
  break the atomicity guarantee in both directions.

- **Cooperative-sticky balancer is safe with `TransactConsumer`**: franz-go
  v1.21's `GroupTransactSession` handles cooperative revokes of zero partitions
  without forcing an abort, so cooperative-sticky is the chosen balancer.

- **Balancer**: cooperative-sticky is used (same as regular consumers) because
  franz-go v1.21 distinguishes a cooperative revoke of zero partitions (no
  abort needed) from an actual revoke, so it does not cause spurious aborts.

- **`RequireStableFetchOffsets`** is permanently enabled in franz-go v1.21+ for
  all group consumers; no explicit opt-in is required.
