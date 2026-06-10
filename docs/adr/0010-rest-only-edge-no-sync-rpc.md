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

## Opening the hatch — checklist

Work through this **per service-pair, per method**, before the first synchronous call ships. A sync call without all four items is a latent cascading failure, not a feature.

- [ ] **Deadline.** Every call site uses `context.WithTimeout` with an explicit per-method budget, and the budget is *smaller* than the caller's own handler deadline minus its remaining work (deadline budgets compose top-down; a callee deadline equal to the caller's is a deadline on paper only). Record the chosen budget in the proto method's comment. Reject configs that disable the timeout.
- [ ] **Retry budget.** Retries only for idempotent methods, only on transport-level failures (unavailable/deadline — never on application errors), capped (1–2 attempts) with jittered backoff, and bounded by a *budget* (e.g. ≤10% of call volume may be retries) so a brown-out cannot be amplified into a storm. Hedging counts against the same budget.
- [ ] **Circuit breaker.** The client is wrapped in `platform/resilience`'s breaker with a defined open-state fallback — degrade (serve stale projection / partial response) or fail-fast with a clear error, chosen per method and written down. The breaker state metric is on the edge dashboard before launch, not after the first incident.
- [ ] **connect-go wiring.** Contract lives in `proto/` and goes through the existing buf pipeline (lint + breaking-change gate); client and handler are buf-generated connect-go code, not hand-rolled HTTP; interceptors propagate auth (same JWKS verification discipline as the edge) and OTel context; transport choice (h2c inside the cluster vs TLS) is explicit. One service-pair at a time — adopting a mesh-wide default re-opens this ADR.
