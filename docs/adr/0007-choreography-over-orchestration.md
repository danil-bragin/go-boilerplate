# ADR 0007 — Choreography over orchestration

**Status:** Accepted  
**Date:** 2026-06-10

## Context

Cross-service workflows (order → payment → notification) need a coordination model. The two candidates: **orchestration** (a central saga coordinator/workflow engine — Temporal, a hand-rolled saga manager — that calls each step and owns compensation) and **choreography** (each service reacts to events and emits its own; nobody owns the whole flow). The demo flow is linear, three hops, with one compensation-free failure branch (`PaymentFailed`, `OrderPaymentTimedOut`).

## Decision

**Choreography.** Services communicate exclusively through events on Kafka; there is no workflow engine and no coordinator service. The known weakness of choreography — "where is my order's flow and why did it stop?" — is answered with **chain lineage instead of a coordinator**: every record carries `correlation-id` (constant per chain, seeded by the gateway as the root command's message id) and `causation-id` (the direct parent's message id), stamped automatically by `consume.Typed` → `outbox.Enqueue` (`platform/messaging/msgctx`). One log/topic search by `correlation-id` reconstructs any chain; OTel traces span the hops.

## Consequences

- No engine to operate, no single point of coordination failure, services stay independently deployable.
- Failure handling is explicit per-edge: `PaymentFailed` events, the unpaid-order deadline watcher, retry tiers + DLT + `cmd/redrive` — not generic engine compensation.
- One compensation case is detected but not automated: a payment charged for an order that already timed out (`PaymentProcessed` after `OrderPaymentTimedOut`) keeps the order timed out and is surfaced as a warn log by the orders payments consumer — the refund is a manual/ops action, not an engine step.
- The flow definition lives in no single file; the projection's status machine and the docs are the closest thing to a flow chart. This is the accepted cost at this scale.

**Revisit triggers** — switch (or add) an orchestrator when any of these become true:

1. A business flow exceeds **~3 hops** or develops **branching compensation** (undo step B only if C and D failed differently) — compensation logic spread over services becomes unmaintainable.
2. The service count reaches **~10** with overlapping flows, where "who consumes what" is no longer reviewable.
3. Flows acquire **human-in-the-loop / long timers** (days) where a workflow engine's durable timers beat hand-rolled watchers.
