-- +goose Up
-- Sharded audit hash chains (ADR-0018): tag each row with its chain and let the
-- chain-head table hold more than one chain (drop the singleton check). chain_id
-- default 1 keeps every write on the single global chain (head id=1) until
-- AUDIT_CHAIN_SHARDS > 1, so this migration is behaviour-preserving. A given
-- actor always maps to one chain, so per-actor audit order is preserved and
-- VerifyChain anchors each chain's genesis + head independently.
-- audit_log is owned by audit_admin after the append-only ownership transfer
-- (00003), so ALTERing it must run as that owner. SET LOCAL ROLE audit_admin
-- (app is a member) when present; otherwise (dev/test superuser) app owns it.
-- +goose StatementBegin
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        set local role audit_admin;
    end if;
    execute 'alter table audit_log add column chain_id smallint not null default 1';
    execute 'create index audit_log_chain_idx on audit_log (chain_id, id)';
    reset role;
end
$$;
-- +goose StatementEnd
-- audit_chain_head stays app-owned; drop the singleton as the migrate role.
alter table audit_chain_head drop constraint audit_chain_head_singleton;

-- +goose Down
-- Rollback only, on a single-row table — the validating scan is trivial.
-- squawk-ignore constraint-missing-not-valid
alter table audit_chain_head add constraint audit_chain_head_singleton check (id = 1);
drop index audit_log_chain_idx;
alter table audit_log drop column chain_id;
