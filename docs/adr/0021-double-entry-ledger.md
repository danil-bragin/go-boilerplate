# ADR 0021 — Double-entry ledger (`platform/ledger`)

**Status:** Accepted
**Date:** 2026-06-14

## Context

platform/money (ADR-0020) gives a precision-exact value type, and the orders /
payments examples store an amount per row. That is the right model for an
e-commerce / PSP flow: an external processor moves the money and the row is a
record. It is the WRONG model when a service is itself the system of record for
balances — wallets, internal credits, an exchange's accounts. There, "what is
the balance of account X" and "do all movements sum to zero" must be answerable
with certainty, and an amount-on-a-row cannot guarantee that money is conserved
across a transfer (two independent row writes can diverge).

The classic answer is a double-entry ledger: every movement is recorded as
balanced debits and credits, balances are derived from the entries, and the
record is append-only. We needed a reusable, money-based implementation —
without forcing it on services that do not need it.

## Decision

Add `platform/ledger`: an append-only, double-entry ledger built on
platform/money. It is **opt-in** — orders/payments do NOT adopt it (their
amounts-on-rows model is documented as sufficient in ADR-0020).

- **Posting / Entry / Account.** A `Posting` is an atomic set of `Entry` legs
  with an `IdempotencyKey`. Each `Entry` is (account, `Direction` debit/credit,
  `Amount money.Money`); the amount is a positive magnitude and the direction
  carries the sign. An `Account` is single-asset with a `Normal` side.

- **Balance invariant, enforced before any write.** `Posting.Validate` requires
  that within each asset the signed sum of entries is exactly zero (debit +,
  credit −). A multi-asset / FX posting balances per asset. This is a pure,
  storage-independent check.

- **Balances are DERIVED, never stored as source of truth.**
  `balance(account) = Σ(entries on Normal side) − Σ(entries on the opposite
  side)`. Because every posting balances per asset, the signed sum of all
  account balances is zero per asset — money is conserved by construction. (A
  cached balance snapshot for read performance is a deliberate future option,
  not v1: correctness first.)

- **Idempotent postings.** A `Posting` carries an `IdempotencyKey`, persisted as
  a UNIQUE column on `ledger_transactions`. Re-posting the same key is a no-op
  (the conflicting insert affects zero rows and the entries are skipped), so
  at-least-once delivery and concurrent posters cannot double-apply. Concurrency
  is safe because the unique index serializes the racing inserts.

- **Append-only.** `ledger_entries` and `ledger_transactions` are never updated
  or deleted; a correction is a new reversing posting. v1 enforces this by
  convention; services wanting hard enforcement transfer table ownership and
  REVOKE update/delete from the app role, exactly as platform/security/audit
  does (ADR-0003-era append-only pattern).

- **Storage.** Three tables (`ledger_accounts`, `ledger_transactions`,
  `ledger_entries`) shipped as an embedded migration (`ledger.Migrations`).
  Amounts are NUMERIC + asset TEXT (the money two-column shape). Two Store
  implementations: an in-memory `MemStore` (tests/examples) and a Postgres
  `PgStore` that resolves the ambient transaction via `pg.FromContext` — so a
  Posting's transaction row and entries commit together when the caller owns a
  transaction (the same invariant as the example repositories).

## Consequences

- Services that own balances get a correct, reusable ledger; those that do not
  keep amounts-on-rows. The choice is explicit (ADR-0020's "when a ledger").
- Money is conserved by construction and provable (a conservation test pins it).
- Postings are exactly-once under retry/concurrency via the idempotency key.
- v1 leaves two things deliberately open, documented in the package and here: a
  cached balance snapshot (read-perf) and hard append-only enforcement via
  ownership transfer. Neither is needed for correctness.

## Alternatives considered

- **Amounts on rows everywhere.** Simple, already used by orders/payments, but
  cannot guarantee conservation for a service that owns balances. Kept for
  PSP-style flows; rejected as the general balance model.
- **Single signed-amount column per movement (no double entry).** Loses the
  balancing invariant and the audit trail of where money came from / went to.
  Rejected.
- **A dedicated ledger engine (e.g. TigerBeetle).** Excellent at scale, but a
  heavy external dependency for a boilerplate; the design here is the in-process,
  Postgres-native starting point and documents the upgrade path.
