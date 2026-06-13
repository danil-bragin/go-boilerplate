# Design — Production-ready pass: collapse migrations + hygiene + verify

**Date:** 2026-06-14
**Status:** Approved (brainstorm), pending implementation plan

## Goal

Bring the boilerplate to a clean production-ready baseline. The template has
never been deployed, so the per-service migration history (features added
incrementally, ~24 files) can be collapsed into a single authoritative baseline
migration per service without any deployed DB to upgrade. Then a bounded hygiene
sweep + full verification.

Three sequential workstreams: (1) collapse migrations (the named, structural
item), (2) targeted hygiene sweep, (3) final verification + green CI.

## Hard rules

1. **Collapsed schema is byte-identical** to applying the full old chain —
   proven by a schema-diff oracle, not assumed.
2. **All behavioural guarantees preserved**: audit append-only ownership
   boundary, HMAC hash-chain + `VerifyChain`, sharded chain_id, partitioned
   outbox from day one, effectively-once outbox/inbox. The existing migration
   tests (append_only, chain_shards, partition, doc-test, squawk) stay the
   behavioural oracle and must pass against the collapsed baseline.
3. **No deployed DB exists** — this is the sole justification for collapsing;
   document it in each baseline's header so a future maintainer never re-collapses
   a live schema.
4. **M=1 byte-identity, race-clean, CI fully green** at the end.
5. Hygiene removes only genuinely dead/leftover code; it does NOT change
   behaviour or public API.

## Workstream 1 — collapse migrations (per service)

Targets (collapse each numbered chain → one `00001_init.sql`):
- `examples/gateway/internal/migrations/sql/` (7 → 1)
- `examples/orders/internal/migrations/sql/` (6 → 1)
- `examples/payments/internal/migrations/sql/` (4 → 1)
- `examples/notifications/internal/migrations/sql/` (1 → 1; already single — normalise header only if needed)
- `platform/security/audit/migrations/` (6 → 1; test-embed used by the audit package's own tests)

### Method (hand-merge, per service)
Read the chain in order, compose ONE `00001_init.sql` whose Up emits all final
DDL in dependency order and whose Down drops it. Preserve VERBATIM the
non-obvious logic that a naive merge would lose:
- **audit_log append-only ownership transfer** — the `do $$ … if exists (audit_admin) … alter table … owner to audit_admin; set local role audit_admin; grant/revoke … reset role; else revoke (no-op for owner) … $$` block. The collapsed baseline must keep the conditional so it works BOTH with init.sql's audit_admin (compose/prod) AND on a bare testcontainer (no role).
- **SET LOCAL ROLE audit_admin** wrapping for any audit_log DDL that runs after the ownership transfer (hash-chain columns, chain_id column + index) — fold those columns into the initial `create table audit_log (...)` where the table is still app-owned BEFORE the transfer, so the SET ROLE dance is only needed for the transfer itself. (Collapsing lets the table be created in its FINAL shape in one statement, then transferred — simpler than the incremental SET-ROLE-per-ALTER the chain needed. The oracle proves equivalence.)
- **audit_chain_head** — genesis row seed; the singleton check was added then dropped across the chain → in the baseline just create it WITHOUT the singleton check (final state, supports sharded chains).
- **partitioned outbox from day one** (ADR-0016) — RANGE partition by `created_at`, the DEFAULT partition, composite PK `(id, created_at)`, the per-partition partial published index. Reproduce the final partitioned shape directly.
- domain tables, inbox dedup table, gateway read-model / pending / product_views, outbox `topic` column, payment tracking columns — all in final shape.

### Oracle verification (one-time, during implementation, per service)
Before deleting the old chain, capture a golden dump; after writing the baseline,
diff:
```
DB_old ← apply OLD chain (from git)         → pg_dump --schema-only → old.sql
DB_new ← apply NEW 00001_init.sql           → pg_dump --schema-only → new.sql
normalise (strip comments/blank lines, sort) ; assert diff(old,new) EMPTY
```
Run on a BARE testcontainer (no audit_admin) so the ownership-transfer branch is
SKIPPED identically in both → the diff isolates pure schema (tables, columns,
types, constraints, indexes, partitions, sequences). The audit_admin-PRESENT
branch is covered separately by the existing `append_only` test run against the
new baseline (creates the role, migrates, asserts ownership transferred). Both
branches thus verified. The oracle is an implementation-time gate (the old files
are gone after collapse); the permanent guard is the existing migration test
suite.

## Workstream 2 — hygiene sweep (bounded)

- **Dead code / unused exports**: grep for unused exported helpers introduced
  this session (e.g. `pg.WrapPool`/`WrapShards` — keep if used by examples/tests,
  remove if orphaned); `go vet` + a deadcode pass (`golang.org/x/tools/cmd/deadcode` if available, else manual).
- **TODO/FIXME inventory**: `grep -rn "TODO\|FIXME\|XXX"` across platform/ +
  examples/ — resolve or convert to a tracked note; no stray "implement later".
- **Naming / comment accuracy**: spot-check comments that drifted (e.g. any that
  still describe two audit modes instead of three; any pre-collapse migration
  references in docs).
- **Doc truth-pass**: README, ARCHITECTURE.md, operations.md, adding-a-service.md,
  conventions.md reflect the FINAL state — including that migrations are now a
  single baseline per service (mention the collapse + the "never re-collapse a
  live schema" rule).
- **Prod-default review** (confirm, do not break): production preflight
  (`RequireProductionSafety`), `config.Secret` redaction, fail-closed limiter,
  auth/JWKS https-enforce, append-only audit — verify still wired; no regression.

## Workstream 3 — verification

`go build ./...`, `go vet ./...`, `golangci-lint run ./...` (0 issues),
`govulncheck ./...`, `-race` on concurrency-critical packages
(`platform/storage/pg`, `platform/security/audit`, `platform/messaging/consume`,
`platform/servicekit`, `platform/messaging/retry`), full suite `-p 1` (platform
+ examples + e2e), commit + push, confirm CI fully green (incl. fast-lane
doc-test/scaffold/errgen, full Test job, squawk SQL-lint over the new baselines).

## Testing

- The schema-diff oracle (W1) — byte-identical schema, per service.
- All existing migration/behaviour tests pass against the collapsed baselines:
  `append_only` (ownership + REVOKE), `chain_shards` (chain_id sharding +
  VerifyChain), partition (outbox partition maintenance + never-drop-unpublished),
  doc-test (adding-a-service blocks compile), squawk (baseline passes the SQL
  linter — a baseline `create table` is a safe op).
- Full regression green at M=1; gated benches (TIER3_BENCH etc.) still pass
  (they migrate the audit embed baseline).

## Out of scope / deferred

- New features; wiring durable-audit (or any opt-in) into the examples by default.
- A fresh full adversarial security audit (bounded variant chosen; code is already
  R8-hardened + reviewed this session).
- Renumbering or restructuring beyond the collapse (no package moves).
- squawk waivers: if squawk flags a baseline statement that is genuinely safe on
  an empty DB (e.g. `not valid` constraint guidance), add a scoped
  squawk-ignore with a comment rather than distorting the schema.
