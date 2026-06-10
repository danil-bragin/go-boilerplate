# ADR 0010 — REST-only edge, no synchronous service-to-service RPC

**Status:** Accepted  
**Date:** 2026-06-10

## Context

The system has exactly one synchronous surface: the gateway's REST API (OpenAPI, oapi-codegen). Between services there are zero sync calls — commands and events travel over Kafka, reads come from the gateway's own projection. The recurring temptation is to add internal RPC (gRPC/ConnectRPC) "for the one place that needs an immediate answer".

## Decision

**No synchronous inter-service RPC for now.** Every cross-service interaction is an event or a command over Kafka. Why not now:

- Sync RPC reintroduces temporal coupling and cascading failure (caller availability = product of callee availabilities) — the exact properties the outbox/inbox + choreography design pays to avoid.
- Every "need a sync read" case so far is better served by a projection (own the data you query) — that pattern is already built and demonstrated.
- A second inter-service protocol means a second contract pipeline, second deadline/retry discipline, second auth story — disproportionate for four services.

**Blessed escape hatch:** when a synchronous call becomes genuinely unavoidable (interactive validation against another service's private data, sub-second read-after-write across a boundary that cannot be projected), the designated tool is **ConnectRPC** — protobuf contracts (reusing the existing buf pipeline and `proto/` discipline), plain HTTP semantics, no separate gRPC ingress requirement, generated clients. Adopt it service-pair by service-pair with explicit deadlines, retry budgets, and circuit breakers (`platform/resilience`); never as a blanket mesh.

## Consequences

- Internal coupling stays asynchronous; backpressure is Kafka lag, not thread-pool exhaustion.
- Some features must be modeled as projections or asynchronous confirmations (the 202 + pending-row pattern) rather than blocking calls — this is a feature, not a workaround.
- When the escape hatch opens, the contract story is already settled (buf + protobuf), so the marginal cost is wiring, not governance.
