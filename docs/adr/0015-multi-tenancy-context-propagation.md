# ADR 0015 — Multi-tenancy: context-propagated tenant id

**Status:** Accepted
**Date:** 2026-06-12

## Context

Multi-tenancy was listed in ARCHITECTURE.md as a "documented seam, not built in
v1": tenant-id context + event propagation. Services that serve more than one
customer organisation need every request, command, event, audit row, and (where
isolation matters) every query scoped to the tenant that originated the work —
and that scope must survive the asynchronous hops the platform already makes for
the principal (`principal-sub`) and chain lineage (`correlation-id`).

The repo already has two precedents for "request-scoped value that rides through
ctx and across Kafka":

- `platform/security/auth` — `Principal` in ctx, `InjectHeaders` /
  `ExtractToContext` over Kafka record headers.
- `platform/messaging/msgctx` — `correlation-id` / `causation-id` in ctx,
  stamped onto outgoing messages by `outbox.Repository.Enqueue`.

Tenant scoping is the same shape, so it follows the same model rather than
inventing a new one.

## Decision

Add `platform/security/tenant`, a single small package that carries one tenant
id through ctx and propagates it end-to-end. Nothing about a service becomes
tenant-aware until it opts in.

### Context carrier + transport (`tenant.go`)

| symbol | purpose |
|---|---|
| `HeaderTenantID = "tenant-id"` | Kafka/outbox record header |
| `WithContext(ctx, id)` / `FromContext(ctx)` | ctx carrier (empty id ⇒ not stored ⇒ absent) |
| `InjectHeaders(ctx, headers)` | producer-side: copy tenant → header (no-op when absent) |
| `ExtractToContext(ctx, headers)` | consumer-side: header → ctx |

### Edge resolution (`middleware.go`)

`tenant.Middleware(claim)` reads the tenant from the **verified JWT principal's
claims** (`claim`, default `"tenant"`) and installs it into the request ctx. It
chains AFTER `auth.Middleware`. Resolution is best-effort — no principal / no
claim ⇒ request proceeds with no tenant — mirroring how `auth.Middleware`
authenticates while `authz.Require` authorizes. The value comes from the
cryptographically verified token, not a client header, so the edge value is
trustworthy.

### Fail-closed enforcement (`guard.go`)

`tenant.Require[C,R]()` is a CQRS behavior that returns
`apperr.CodeTenantRequired` (`TENANT_REQUIRED`, 400, permanent) when ctx carries
no tenant, before the handler runs. Place it INSIDE auth/authz but OUTSIDE the
transaction so a missing tenant is rejected before any DB work. Permanent ⇒ on
the async path the tiered-retry escalator short-circuits it straight to the DLT.

### Automatic propagation (zero per-service code)

- `consume.Typed` calls `tenant.ExtractToContext` right after
  `auth.ExtractToContext`, so every typed handler runs with the originating
  tenant in ctx.
- `outbox.StampChainHeaders` (and thus `Enqueue`) calls `tenant.InjectHeaders`
  under the same "explicit headers win" rule as the principal, so every emitted
  event carries the tenant across the next hop with no handler involvement.
- `audit.Audit` records the tenant in the entry's `metadata` map (`tenant_id`).
  Metadata is already part of the hash chain, so tenant attribution is
  tamper-evident with **no schema change and no chain-format change** — chosen
  over a dedicated `audit_log.tenant_id` column precisely to avoid touching the
  genesis/hash-chain invariant.

## Consequences

- A service becomes multi-tenant by wiring three opt-in points: chain
  `tenant.Middleware` at the edge, add `tenant.Require` to tenant-scoped command
  pipelines, and filter its own repository queries by `tenant.FromContext`.
  Propagation through Kafka, outbox, and audit is automatic.
- **The `tenant-id` Kafka header is transport metadata, NOT authentication** —
  identical trust boundary to `principal-sub` (ADR-0014). Any producer with
  topic write rights can forge it; the perimeter is the broker SASL/ACL/mTLS.
  Never make an isolation decision from the propagated header for data that may
  originate outside that perimeter; at the edge the tenant is derived from the
  verified JWT, which is the trustworthy source.
- **Database isolation (Postgres RLS) is deferred.** This ADR establishes the
  context + propagation seam; row-level-security policies keyed on a
  `SET app.tenant_id` GUC, and `tenant_id` columns on domain tables, are a
  follow-up that builds on `tenant.FromContext`. Until then, query-level
  filtering in repositories is the isolation mechanism.
- The production preflight (`ProductionGuard`) is the natural place to later
  require that tenant-scoped services run with enforcement on when
  `APP_ENV=production`.
- Single-tenant deployments pay nothing: no middleware wired ⇒ no tenant in ctx
  ⇒ `InjectHeaders`/audit metadata stay empty and `Require` is simply not used.
