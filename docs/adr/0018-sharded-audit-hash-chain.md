# ADR 0018 — Sharded audit hash chain (actor-keyed)

**Status:** Accepted
**Date:** 2026-06-12

## Context

The audit hash chain (ADR-0014 work) serializes every audit write on a single
`audit_chain_head` row locked `FOR UPDATE` inside the command transaction. That
is what makes the chain total and gap-free — and `VerifyChain` relies on it —
but it is a **global** serialization point: two commands by unrelated actors on
unrelated aggregates still queue on the same chain-head lock. Because the audit
behavior runs *inside* the command transaction, the audit lock caps
command throughput across the whole service, independent of which rows the
commands touch. This is the last of the three Tier-2 global serialization points
(after the single-active outbox relay, ADR-0017) called out in
`docs/operations.md` § Scaling. The code already named the escape hatch:
"per-actor chains are the escape hatch when this becomes the bottleneck."

A hash chain is inherently serial — you cannot parallelize a *single* chain
(each entry's `prev_hash` is the previous entry's `entry_hash`). The only way to
parallelize is to have **more than one chain**.

## Decision

Add **opt-in chain sharding** (`AUDIT_CHAIN_SHARDS`, default 1), keyed by actor.

- **N chains, actor-hashed.** `chain_id = 1 + fnv32(actor) % N`. Chains occupy
  head ids `[1, N]`; the default single chain is id 1 (the genesis head the
  migration already seeds), so `N = 1` is the existing behaviour exactly. A
  given actor maps to one chain for life, so **per-actor audit order is
  preserved** — the property that matters for an audit trail.
- **Per-chain head lock.** `Record` resolves the actor's `chain_id`, lazily
  creates that chain's genesis-seeded head row (`ON CONFLICT DO NOTHING`, skipped
  for the single-chain default), and locks `audit_chain_head WHERE id = chain_id
  FOR UPDATE`. Writes by actors on different chains no longer contend → ~N×
  audit write throughput.
- **Per-chain verification.** `audit_log` carries a `chain_id` column.
  `VerifyChain` walks each chain independently (`verifyOneChain`), anchoring that
  chain's genesis (first row → zero root) and head (last row →
  `audit_chain_head[chain_id].last_hash`), and runs the same entry-hash + link
  checks. A break in any chain is reported. The hash itself is unchanged —
  `computeEntryHash` does NOT include `chain_id`, so tamper-evidence semantics
  are identical, just applied per chain.

### Backward compatibility

`N = 1` (default) is **behaviour-preserving**: every write lands on chain 1, the
lazy-create is skipped, and the lock/insert/update/verify paths are equivalent
to the single global chain. The 9 existing chain tests (clean verify, tamper,
deletion, head truncation, keyed forge, concurrent writers, …) pass unchanged.
The schema migration adds `chain_id smallint NOT NULL DEFAULT 1` and drops the
`audit_chain_head` singleton check — additive, no rewrite of the hash logic.

## Consequences

- Going 1 → N is a config flip (`AUDIT_CHAIN_SHARDS=N`) plus the additive
  migration; no chain-format change. Changing N later re-buckets actors to
  different chains for *new* rows; since order is only required per actor and an
  actor never splits across two chains at once, this is safe between deploys.
- Tamper-evidence is **unchanged in strength**, now enforced per chain. An
  attacker editing/deleting a row still breaks exactly the chain that row
  belongs to, and `VerifyChain` walks every chain.
- Sharding helps **audit-write contention**, not the single-Postgres-writer
  ceiling (ADR-0008). Combined with outbox relay sharding (ADR-0017) it removes
  the two cross-aggregate serialization points the audit + relay imposed; the
  remaining ceiling is the writer itself (Tier-3: shard Postgres).
- Audit metrics stay global; chain identity is implicit in the `audit_log.chain_id`
  column for forensic queries.
- The keyed-HMAC forgery-resistance (`AUDIT_CHAIN_KEY`) composes unchanged — each
  chain is keyed with the same secret.
