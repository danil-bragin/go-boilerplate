-- +goose Up
-- Durable-audit staging (see docs/superpowers/specs/2026-06-13-durable-audit-design.md).
-- A Durable command inserts an audit INTENT here inside its own transaction
-- (cheap, no chain-head lock); a single-active per-shard worker drains these
-- into the append-only audit_log hash chain exactly-once. Owned by the app role:
-- unlike audit_log (append-only, audit_admin-owned), this is transient staging
-- the app must INSERT and the drain must DELETE.
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

-- +goose Down
drop table audit_pending;
