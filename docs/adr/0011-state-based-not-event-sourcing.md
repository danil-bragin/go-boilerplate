# ADR 0011 — State-based persistence, not event sourcing

**Status:** Accepted  
**Date:** 2026-06-10

## Context

A Kafka-centric architecture invites the question: are events the source of truth (event sourcing, replayable log, derived state) or a transport? The answer dictates retention policy, schema discipline, rebuild procedures, and how much Kafka operations the team must master.

## Decision

**Database rows are the source of truth; Kafka is transport.** Each service's Postgres tables hold current state; events announce state changes (via the transactional outbox) so others can react. Topics have finite retention (`TOPIC_RETENTION`, default 7 days) and are NOT a system of record. Aggregates are never rebuilt from the log; the gateway projection is rebuilt from *recent* events only (see the rebuild runbook and its retention caveat in `docs/operations.md`).

**Documented asymmetry — the edge direct-produce exception:** the gateway produces `CreateOrderCommand` directly to Kafka (`resilience.Do` retry ×3 + 2 s timeout) instead of going through an outbox, because the gateway has no domain table that the command would change — there is nothing to commit atomically with. The compensations: 202 semantics (accepted, not done), a deterministic order id (Idempotency-Key → UUIDv5) so client retries collapse downstream via the inbox, and a `pending` projection row written after the produce. Every *domain* state change in orders/payments still flows through the outbox. This is the only direct-produce path and must stay that way.

**Analytics:** do not point analytics at the 7-day event topics and do not query service databases directly. The recommended pattern is **CDC-to-warehouse**: Debezium (or equivalent) on each service's Postgres → warehouse/lake (with the outbox table as a clean, already-versioned change feed where event-shaped data is wanted). That keeps the warehouse complete and replayable without turning Kafka into an archive.

## Consequences

- No event-store operational burden: no infinite retention, no compaction tuning as a correctness concern, no upcaster chains. Restoring a service = restoring its database (plus the DR caveats in `docs/operations.md`).
- Audit history is the `audit_log` table + finite Kafka history, not a full domain replay. Teams needing true temporal reconstruction should adopt event sourcing for that bounded context explicitly — this ADR records that the default is not that.
- Consumers may rely on events being *recent*; anything needing full history belongs behind CDC.
