-- +goose Up
-- DO NOT partition this table for retention. Unlike outbox (ADR-0016),
-- audit_log is append-only and hash-chained: VerifyChain asserts a genesis
-- anchor (first row links to the zero root) and a head anchor, which are
-- truncation-detection checks. Partition retention DROPs the oldest partition
-- — i.e. deletes the genesis row — which is exactly the tampering VerifyChain
-- is built to flag. Partition-for-purge and a tamper-evident chain are
-- irreconcilable. Retention stays age-based via the privileged admin pool.
create table audit_log (
    id          bigserial primary key,
    actor       text        not null,
    action      text        not null,
    subject     text        not null,
    metadata    jsonb       not null default '{}'::jsonb,
    created_at  timestamptz not null default now()
);
create index audit_log_action_idx on audit_log (action, created_at);

-- +goose Down
drop table audit_log;
