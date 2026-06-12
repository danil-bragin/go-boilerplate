# ADR 0016 — Outbox partitioning (opt-in), and why audit is excluded

**Status:** Accepted
**Date:** 2026-06-12

## Context

The outbox is the highest-write table in the system: every state-changing
command inserts a row, the relay marks it published, and retention later removes
it. Age-based cleanup (`DeletePublishedBefore`, a polling `DELETE ... WHERE
published_at < $1`) keeps the table bounded, but at high throughput a bulk
`DELETE` is the wrong tool: it leaves dead tuples for autovacuum to chase, bloats
the heap and the partial indexes, and competes with the relay's hot path for I/O
and locks. The standard fix is **range partitioning by time + DROP old
partitions**: dropping a partition is an instant catalog operation that reclaims
space with zero dead tuples and zero autovacuum work.

Two constraints shape the decision:

1. **Most installs do not need it.** Partitioning adds operational surface
   (partitions must be pre-created ahead of time or inserts fail). Forcing it on
   every consumer of the boilerplate is the wrong default.
2. **It must not become a hard migration later.** Converting a live
   non-partitioned table to partitioned requires a table rewrite + data move.
   That cost should be paid once, up front, not deferred onto users.

## Decision

### Outbox is partitioned from day one; `simple` mode is the default behaviour

The `outbox` table is declared `PARTITION BY RANGE (created_at)` with a composite
`PRIMARY KEY (id, created_at)` (a partitioned table's PK must include the
partition key) and a single **DEFAULT partition** (`outbox_default`).

- **`created_at`, not `published_at`, is the partition key.** `created_at` is
  immutable, so a row never moves partitions. `published_at` is set *after*
  insert, so partitioning on it would force row movement across partitions on
  every publish — expensive and pointless.
- **simple mode (default):** no maintenance worker runs. Every row lands in
  `outbox_default`, the table behaves exactly like the old non-partitioned
  outbox, and age-based `DeletePublishedBefore` remains the cleanup mechanism.
  Zero operational overhead, identical query plans.
- **partitioned mode (opt-in):** enable the maintenance worker and set an
  interval + retention. The worker pre-creates time-range partitions ahead of
  `now`, so new rows route into them and `outbox_default` stays empty in steady
  state; it then `DETACH`+`DROP`s partitions older than retention.

Because the PK and `PARTITION BY` are already in place, **switching
simple→partitioned is a config change, not a schema migration** — no PK change,
no table rewrite. Future partitions cover `created_at >= now`, which never
overlaps the historical rows already sitting in `outbox_default`, so creating
them never conflicts; those legacy rows are drained by the existing age-based
`DELETE`.

### Maintenance worker (`platform/messaging/outbox` partition manager)

A `PartitionManager` wired via `servicekit` as a **single-active** periodic
worker (leader-elected through the same advisory lock as the relay). Each tick:

1. **Ensure** partitions exist from the current interval through
   `precreate_lookahead` intervals into the future (idempotent `CREATE TABLE …
   PARTITION OF … FOR VALUES FROM (…) TO (…)`).
2. **Drop** partitions whose entire range is older than `retention`.

**Hard safety invariant (enforced in code, covered by a test):** the worker
**NEVER** drops a partition that still holds an unpublished row
(`published_at IS NULL`). It probes each drop candidate first and, if any
unpublished row remains, **skips and increments a metric** rather than dropping —
a slow or stalled relay can never cause silent event loss. The `DEFAULT`
partition is never dropped.

Observability: gauges for partition count and oldest/newest bound, counters for
partitions created / dropped / skipped-due-to-unpublished, and worker run
duration.

### Configuration (env, not hardcoded)

| env | default | meaning |
|---|---|---|
| `OUTBOX_PARTITION_MODE` | `simple` | `simple` (DEFAULT partition only, no worker) \| `partitioned` (worker on) |
| `OUTBOX_PARTITION_INTERVAL` | `720h` (~1 month) | partition width |
| `OUTBOX_PARTITION_RETENTION` | `2160h` (~3 months) | drop partitions older than this |
| `OUTBOX_PARTITION_LOOKAHEAD` | `2` | intervals of future partitions to pre-create |

### Relay strategy is orthogonal to partitioning

The polling relay (`SELECT … FOR UPDATE SKIP LOCKED WHERE published_at IS NULL` +
the partial index) is **untouched** — partitioning is transparent to it
(`WHERE id = …` still resolves via the composite PK; the partial index is
propagated to every partition). A future WAL/CDC relay strategy (e.g. Debezium
reading the replication slot) would be equally compatible: with CDC, partitioning
affects only retention, never the read path. This ADR deliberately does **not**
modify the proven hot relay path.

## Why `audit` is NOT partitioned (and must never be)

It is tempting to apply the same recipe to `audit_log`. **Doing so would break
the round-8 audit tamper-evidence guarantees (ADR-0014 work):**

- `audit_log` is append-only (ownership-revoked from the app role) and carries a
  hash chain whose `VerifyChain` walk asserts a **genesis anchor** (the first
  row links to the all-zero root) and a **head anchor** (the last row equals
  `audit_chain_head.last_hash`). These are explicit **truncation-detection**
  checks: deleting old rows is supposed to be *detected as tampering*.
- Partition retention works by `DROP`ping the oldest partition — i.e. **deleting
  the oldest audit rows, including the genesis row**. That is precisely the
  mutation `VerifyChain` is designed to flag (`genesis anchor mismatch`).
  Partition-for-purge and a tamper-evident chain are fundamentally
  irreconcilable: the chain's value is that you *cannot* silently drop history.

Therefore the partitioning recipe is **structurally inapplicable to audit**. The
partition manager lives in the `outbox` package only; `audit` retains age-based
cleanup through its privileged (`PG_AUDIT_ADMIN_URL`) pool. Genuine audit
archival (move-then-prune with a deliberate, audited re-anchor) is a separate,
out-of-scope operation. A comment next to the audit schema records this so no one
re-derives the bad idea.

## Consequences

- Default installs pay nothing: one DEFAULT partition, same plans, same cleanup.
- Going to partitioned retention is a config flip (`OUTBOX_PARTITION_MODE=
  partitioned` + worker wired) with no migration.
- The never-drop-with-unpublished invariant makes the worker safe to run beside a
  lagging relay.
- Signals to switch (documented in `operations.md`): outbox heap/index bloat,
  autovacuum lag on `outbox`, or rising publish-poll P99 under DELETE churn.
