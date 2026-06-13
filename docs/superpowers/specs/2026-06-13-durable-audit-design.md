# Design — Durable audit (staging + drain worker)

**Date:** 2026-06-13
**Status:** Approved (brainstorm), pending implementation plan

## Goal

Add a third audit consistency mode — **Durable** — that loses NOTHING (unlike A2
Eventual's best-effort drop-on-overflow) yet keeps the audit hash-chain
`FOR UPDATE` serialization OFF the command hot path (unlike Strong's sync
audit-in-tx). The command writes a cheap, durable audit *intent* inside its own
transaction; a single-active per-shard worker drains intents into the hash chain
asynchronously and exactly-once.

This is the "outbox pattern applied to audit", done locally (no Kafka — the
`audit_log` is per-service, so cross-process decoupling buys nothing). It also
incidentally lifts the measured Strong order-create ceiling (~1.6k/s/shard,
bounded by the in-command chain-head lock): a Durable command pays only a cheap
local insert, so audit stops serializing the write path.

## The three audit modes (after this work)

| Mode | Behavior | Guarantee | Hot-path cost |
|---|---|---|---|
| Strong | `audit.Audit` (exists) | in the chain at commit; can't-audit→don't-commit | chain-head `FOR UPDATE` in the command tx |
| Eventual | `audit.AsyncAudit` (exists, A2) | best-effort, MAY DROP on buffer overflow | none (async in-process) |
| **Durable** (new) | `audit.DurableAudit` | effectively-once, NEVER drops, applied async (lag) | one cheap local insert (no chain lock) |

The caller picks the behavior, exactly as A2 established (cqrs cannot import
audit, so the audit axis is the caller's choice). `ConsistencyPolicy` is
UNCHANGED. Durable requires a Transactional command (the intent insert must be in
the command tx) — documented, like sync `Audit`.

## Hard rules

1. **No loss.** A committed command's audit intent is durable (a row in the
   command's transaction); it is applied to the chain or still pending — never
   dropped. Crash between commit and drain loses nothing (the intent is in
   Postgres).
2. **Exactly-once apply.** Each intent lands in the chain exactly once: the drain
   applies and deletes the pending rows in ONE transaction, and the drain is
   single-active per shard (a hash chain must be applied strictly in order — two
   concurrent drainers could apply one chain's rows out of order and break the
   `prev_hash` linkage).
3. **Tamper-evidence preserved.** The worker builds the chain with the same
   `computeEntryHash` and `RecordBatchSameChain`; `VerifyChain` passes. The hashed
   `at` is the ORIGINAL event time captured at intent-insert (µs-truncated before
   hashing — the standing rule), NOT the apply time.
4. **Strong/Eventual unchanged.** `audit.Audit` and `audit.AsyncAudit` are not
   modified; Durable is additive and opt-in.

## Components

### 1. Migration — `audit_pending`
A staging table, per service (inlined alongside the existing audit_log
migrations) plus a platform copy for the audit package's tests:
```sql
create table audit_pending (
    id         bigserial   primary key,
    chain_id   smallint    not null,
    actor      text        not null,
    action     text        not null,
    subject    text        not null,
    metadata   jsonb,
    created_at timestamptz not null
);
create index audit_pending_drain_idx on audit_pending (chain_id, id);
```
Owned by the **app** role (it is staging, not the append-only chain itself — app
must INSERT and the drain must DELETE). The chain (`audit_log`) keeps its
append-only ownership. `created_at` is the original event time (µs-truncated),
carried through so the chain hash reflects true event time.

### 2. `audit.DurableAudit[C,R](store *PgStore, action string, subjectFor func(C) string) cqrs.Behavior[C,R]`
The Eventual/Strong sibling. After the handler succeeds, it builds the same
`Entry` (actor from ctx, action, subject, tenant metadata) and calls
`store.InsertPending(ctx, entry)` — a single INSERT into `audit_pending` on the
ambient transaction (the command's tx, so it commits atomically with the command
and never touches the chain-head lock). It stamps `chain_id = chainIDFor(actor)`
and `created_at = now().UTC().Truncate(µs)`. It does NOT fail the command on an
insert error any more or less than sync `Audit` does — match `Audit`'s
error contract (an InsertPending error rolls the command back, because a Durable
command's contract is "the intent is committed with me"; this is the durability
guarantee, distinct from Eventual's best-effort). Document this clearly.

### 3. `(*PgStore).InsertPending(ctx, Entry) error`
Marshals metadata, normalises `at` (zero→now, truncate µs), computes
`chain_id = s.chainIDFor(entry.Actor)`, inserts one `audit_pending` row via
`pg.FromContext(ctx, s.pool)` (ambient tx). No chain-head interaction.

### 4. `(*PgStore).DrainPending(ctx, batchSize int) (applied int, err error)`
One drain pass:
- `select id, chain_id, actor, action, subject, metadata, created_at from audit_pending order by chain_id, id limit $batchSize` (read on the writer pool / inside a tx).
- Group rows by `chain_id` (preserving id order within a chain).
- For each chain group, in ONE transaction (`pg.RunInTx`): `RecordBatchSameChain(ctx, chain_id, entries)` then `delete from audit_pending where id = any($ids)`. Apply+delete atomic ⇒ exactly-once; a failure rolls back both (re-drained next pass).
- Returns the count applied. Loops/paginates until fewer than batchSize remain or the caller stops.
Ordering within a chain = the `id` sequence = enqueue order (the InsertPending order = handler completion order). The chain hash links in that order.

### 5. `servicekit.AddAuditDrain(store, interval, batchSize)`
A single-active (leader-elected) periodic worker, registered PER physical shard
(loops `s.shards.Shards()`, like `AddOutboxRelay`; the audit store/chain is
per-shard). Single-active because the chain must be applied serially per chain.
Reuses `pg.RunAsLeader` / the existing leader pattern + `AddPeriodicWorker`. The
leader lock key is shard-distinct (`:pshard:<i>` at M>1, like the relay). At M=1
one worker, identical semantics. The worker calls `DrainPending` each tick;
on error it logs + the rows stay pending (retried next tick — no loss).

## Data flow

```
command tx { domain write + outbox enqueue + store.InsertPending }
   → commit            (durable; NO chain-head lock; cheap)
drain worker (leader, per shard, every `interval`):
   DrainPending: select pending by (chain_id,id) → per chain:
       RecordBatchSameChain + delete applied  → commit  (exactly-once, ordered)
VerifyChain: walks the chain the worker built — passes (same hashing).
```

## Error handling

- InsertPending error in the command tx → command rolls back (durability
  contract: a committed Durable command has its intent staged).
- DrainPending error on a chain group → that group's tx rolls back, rows stay
  pending, retried next tick. No loss, no partial chain.
- Worker stops cleanly on ctx cancel (drains in-flight bounded); leader handoff
  on instance loss (existing single-active machinery).
- Backlog visibility: a metric `audit.pending_backlog` (gauge, rows in
  audit_pending) so operators see drain lag; alert if it grows unbounded.

## Testing

- **No-loss / durability:** InsertPending N intents, do NOT drain, assert N rows
  in audit_pending; then drain → N rows in audit_log, 0 pending; VerifyChain OK.
  Simulate "crash" = a fresh store/worker over the same DB drains the survivors.
- **Exactly-once:** drain twice → no duplicate chain rows (pending deleted);
  VerifyChain OK; chain length == N.
- **Order/tamper-evidence:** multi-entry single chain drained → VerifyChain OK;
  the `at` in the chain equals the original intent time (not apply time).
- **DurableAudit behavior:** after a successful handler the intent is in
  audit_pending; on handler error nothing is staged (match `Audit`).
- **Throughput bench (gated):** Durable command-rate (intent insert in tx) vs
  Strong (sync chain-lock in tx), single actor, 1 shard — Durable materially
  higher (chain-lock off the hot path); the drain keeps up or backlog is
  reported. Extend the existing strong/eventual sharding bench with a "durable"
  row.

## Out of scope / deferred

- Rewiring the money path to Durable by default — sync `Audit` stays the default;
  Durable is opt-in per command. (The boilerplate user chooses per compliance
  need: strict in-chain-at-commit → Strong; durable + lag → Durable; high-volume
  droppable → Eventual.)
- Kafka / cross-process audit transport — rejected (audit_log is per-service
  local; staging+worker achieves durability with far less plumbing).
- Centralized audit service / global cross-service chain.
- audit_pending retention/backpressure beyond the drain (the drain empties it;
  if the worker is down the table grows — surfaced by the backlog metric).
