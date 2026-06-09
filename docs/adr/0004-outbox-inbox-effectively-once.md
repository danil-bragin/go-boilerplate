# ADR 0004 — Reliable messaging: outbox + inbox over Kafka EOS

**Status:** Accepted  
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
- The polling relay introduces a small additional latency (default 1 s poll interval, configurable via `OUTBOX_POLL_INTERVAL`). CDC-based relay (Debezium) is the documented scale-out path for sub-second latency at high throughput.
- Services must have an `outbox_messages` table and an `inbox` table in their migrations (the `orders` service is the canonical example).
