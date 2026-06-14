-- +goose Up
-- Canonical schema for platform/ledger. Append-only double-entry: accounts,
-- balanced transactions, and entries. Adopters apply this via
-- pg.Migrate(ctx, dsn, ledger.Migrations, "migrations") or copy it into their
-- own migration chain. Amounts are NUMERIC (platform/money two-column shape:
-- amount + asset), never float (ADR-0020, ADR-0021).

create table ledger_accounts (
    id         text        primary key,
    asset      text        not null,
    normal     text        not null check (normal in ('debit', 'credit')),
    created_at timestamptz not null default now()
);

-- One row per posting; the unique idempotency_key makes Post exactly-once
-- (a re-posted key conflicts and is a no-op).
create table ledger_transactions (
    id              uuid        primary key,
    idempotency_key text        not null unique,
    created_at      timestamptz not null default now()
);

-- One row per entry (leg). amount is a positive magnitude; direction carries
-- the sign. APPEND-ONLY: never UPDATE/DELETE (a correction is a new reversing
-- posting). For hard enforcement, transfer ownership + REVOKE update,delete
-- from the app role as platform/security/audit does.
create table ledger_entries (
    id         bigserial   primary key,
    tx_id      uuid        not null references ledger_transactions (id),
    account_id text        not null references ledger_accounts (id),
    direction  text        not null check (direction in ('debit', 'credit')),
    amount     numeric     not null check (amount > 0),
    asset      text        not null,
    created_at timestamptz not null default now()
);

create index ledger_entries_account_idx on ledger_entries (account_id);

-- +goose Down
drop table ledger_entries;
drop table ledger_transactions;
drop table ledger_accounts;
