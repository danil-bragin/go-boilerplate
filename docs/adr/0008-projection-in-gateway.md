# ADR 0008 — Read-model projection embedded in the gateway

**Status:** Accepted  
**Date:** 2026-06-10

## Context

The gateway serves REST reads from a Kafka-fed read model (`orders_read`). That projection consumer has to run somewhere: inside the gateway binary, or as a separate deployable. A separate deployable is the textbook CQRS answer but doubles the operational footprint of the demo for no load reason.

## Decision

**Embedded by default, split seam built in.** The gateway runs the projection consumer in-process when `GATEWAY_EMBEDDED_PROJECTION=true` (the default). The projection wiring lives in its own package and compiles into a standalone consumer-only binary, `examples/gateway/cmd/projection` (servicekit harness, admin server only, no public HTTP). To split: deploy `cmd/projection`, set `GATEWAY_EMBEDDED_PROJECTION=false` on the gateway. Both modes share the same `gateway-projection` consumer group and inbox table, so the handover is a plain consumer-group rebalance — no offset surgery, no dual writes.

## Consequences

- One deployable in dev and small production setups; HTTP reads and projection writes share a pool and a release cadence.
- Embedded mode couples edge scaling to projection consumption: every gateway replica is a consumer-group member, so scaling replicas for HTTP traffic redistributes partitions too.

**When to split** (`GATEWAY_EMBEDDED_PROJECTION=false` + `cmd/projection`):

1. HTTP autoscaling churns the consumer group (rebalance storms on scale events).
2. Projection lag needs dedicated CPU/replica budget independent of edge traffic.
3. Read-model rebuild/replay (see `docs/operations.md`) should not run inside latency-sensitive edge pods.
4. Different release cadence: projection logic changes more often than the edge contract (or vice versa).

## Amendment (2026-06-10) — split mode is NOT a pure edge

Even with `GATEWAY_EMBEDDED_PROJECTION=false`, `POST /v1/orders` still touches
the read-model database by design: it inserts the `pending` row
(read-your-writes — an immediate GET must not 404) and performs the
idempotency body-mismatch lookup. Together with the projection's ~3
transactions per order (inbox + OrderCreated upsert, then each payment-outcome
event), the shared read-model **writer** is the aggregate throughput ceiling —
roughly 2–3.5k orders/s on a mid-size single instance — **independent of
gateway replica count**. Splitting the projection moves CPU and consumer-group
membership out of the edge pods; it does not remove this ceiling.

Levers, in escalation order: `PG_READER_DSN` (idempotency lookups move to the
reader pool — bounded staleness documented in `openapi.yaml`),
`GATEWAY_PENDING_ASYNC=true` (pending inserts become one batched multi-row
INSERT per ≤50 ms/≤100 rows), table partitioning of `orders_read`, then a
bigger writer (e.g. Aurora). See `docs/operations.md` § Scaling guide.
