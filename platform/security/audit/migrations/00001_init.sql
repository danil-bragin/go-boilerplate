-- +goose Up
-- Collapsed baseline — never deployed; merged from the original chain.
-- DO NOT re-collapse a live schema.
--
-- This is the final state of the original 6-migration chain folded into one
-- baseline: 00001_audit (audit_log + action index), 00002 (actor index),
-- 00003 (append-only ownership transfer do-block), 00004 (hash chain:
-- prev_hash/entry_hash + audit_chain_head + genesis), 00005 (chain shards:
-- chain_id + chain index + drop of the chain-head singleton check), and
-- 00006 (audit_pending staging). The chain-head singleton check is therefore
-- ABSENT here — it was added in 00004 then dropped in 00005, so the final
-- state has no singleton constraint.

-- DO NOT partition this table for retention. Unlike outbox (ADR-0016),
-- audit_log is append-only and hash-chained: VerifyChain asserts a genesis
-- anchor (first row links to the zero root) and a head anchor, which are
-- truncation-detection checks. Partition retention DROPs the oldest partition
-- — i.e. deletes the genesis row — which is exactly the tampering VerifyChain
-- is built to flag. Partition-for-purge and a tamper-evident chain are
-- irreconcilable. Retention stays age-based via the privileged admin pool.
--
-- prev_hash/entry_hash (hash-chain tamper-evidence, originally 00004) and
-- chain_id (sharded chains, originally 00005) are folded into the create:
--   prev_hash  — entry_hash of the immediately preceding row in the chain
--   entry_hash — sha256(prev_hash || canonical(actor,action,subject,at,metadata))
--   chain_id   — which chain this row belongs to (default 1 = single global
--                chain until AUDIT_CHAIN_SHARDS > 1; ADR-0018)
create table audit_log (
    id          bigserial primary key,
    actor       text        not null,
    action      text        not null,
    subject     text        not null,
    metadata    jsonb       not null default '{}'::jsonb,
    created_at  timestamptz not null default now(),
    prev_hash   bytea,
    entry_hash  bytea,
    chain_id    smallint    not null default 1
);
create index audit_log_action_idx on audit_log (action, created_at);
-- Backs the DSAR/audit read path (PgStore.Query / QueryByActor): one actor's
-- trail filtered by created_at, newest first.
create index audit_log_actor_idx on audit_log (actor, created_at);
-- A given actor always maps to one chain; VerifyChain anchors each chain's
-- genesis + head independently (ADR-0018).
create index audit_log_chain_idx on audit_log (chain_id, id);

-- Hash-chain head. Chain scope is per-chain_id: a row here per chain holds the
-- entry_hash of that chain's latest row, locked FOR UPDATE inside the audit
-- insert transaction so the chain order is total and gap-free under concurrent
-- writers. No singleton check (00005 dropped it to allow >1 chain head row).
create table audit_chain_head (
    id         smallint primary key default 1,
    last_hash  bytea       not null,
    updated_at timestamptz not null default now()
);

-- Genesis: an all-zero 32-byte hash is the prev_hash of the first real entry.
insert into audit_chain_head (id, last_hash)
values (1, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);

-- The app role reads + advances the chain head on every audit write (and the
-- sharded mode lazily INSERTs new per-chain head rows — ADR-0018). Grant those
-- to the DATABASE OWNER (the runtime app role) explicitly, so it works whether
-- migrations run as the app role (which would own the table anyway) or as a
-- privileged/superuser migrate role (PG_MIGRATE_URL), where the table would
-- otherwise be owned by the migrate role and leave the app without access.
-- +goose StatementBegin
do $$
declare
    app_role text := (select pg_catalog.pg_get_userbyid(datdba)
                      from pg_database where datname = current_database());
begin
    execute format('grant select, insert, update on audit_chain_head to %I', app_role);
end
$$;
-- +goose StatementEnd

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

-- Append-only audit_log: the application role may INSERT and SELECT but never
-- UPDATE or DELETE. This makes the table tamper-resistant at the database
-- privilege boundary — even a fully compromised app connection cannot rewrite
-- or erase history through the ORM/SQL path.
--
-- OWNERSHIP IS LOAD-BEARING. A table OWNER keeps UPDATE/DELETE implicitly —
-- REVOKE cannot strip an owner's own rights — so if the app role owns audit_log
-- the REVOKE below is a no-op and the append-only guarantee is fiction. When a
-- dedicated "audit_admin" role exists (deploy/postgres/init.sql provisions it),
-- this migration TRANSFERS OWNERSHIP of audit_log + its sequence to audit_admin
-- and re-grants the app role only INSERT + SELECT. Now the app is a NON-OWNER
-- whose UPDATE/DELETE the REVOKE actually denies, and audit_admin (the
-- retention role, dialed via PG_AUDIT_ADMIN_URL) keeps DELETE for age-based
-- pruning. Where no audit_admin role exists (single-role local dev; the test
-- container's superuser app role) ownership transfer is skipped and the REVOKE
-- is best-effort — proven meaningless for a superuser/owner by
-- append_only_test, which exercises a dedicated NON-OWNER role.
--
-- TRADE-OFF: once audit_admin owns audit_log, future ALTERs of audit_log must
-- run as audit_admin/superuser, not the app role. That is intentional — the
-- app must not be able to restructure the audit table either.
--
-- Runs LAST: the ownership transfer + REVOKE must apply to the fully-built
-- audit_log (all columns/indexes already created above).
-- +goose StatementBegin
do $$
-- Grant/revoke target the DATABASE OWNER (the runtime app role — init.sql:
-- CREATE DATABASE <svc> WITH OWNER app). After audit_admin takes ownership the
-- app role is a NON-owner and can no longer GRANT/REVOKE on audit_log, so we
-- SET LOCAL ROLE audit_admin (app is a member — GRANT audit_admin TO app — so
-- SET ROLE works even with INHERIT FALSE) and run the owner-side statements as
-- audit_admin. Correct whether migrations run as the app role (the default) or
-- any role that is a member of audit_admin.
declare
    app_role text := (select pg_catalog.pg_get_userbyid(datdba)
                      from pg_database where datname = current_database());
begin
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        -- Hand the table + its owned sequence to the privileged role so the app
        -- role becomes a non-owner the REVOKE can actually constrain.
        execute 'alter table audit_log owner to audit_admin';
        -- Act AS the new owner to re-grant the app role and constrain it.
        set local role audit_admin;
        execute format('grant select, insert on audit_log to %I', app_role);
        execute format('grant usage, select on sequence audit_log_id_seq to %I', app_role);
        execute format('revoke update, delete on audit_log from %I', app_role);
        reset role;
    else
        -- No audit_admin (single-role dev / the test superuser): app owns
        -- audit_log, so the REVOKE is a documented no-op for an owner.
        execute format('revoke update, delete on audit_log from %I', app_role);
    end if;
end
$$;
-- +goose StatementEnd

-- +goose Down
-- Restore app-role ownership/rights, then drop everything in reverse order.
-- +goose StatementBegin
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        execute 'alter table audit_log owner to ' || quote_ident(current_user);
    end if;
    execute format('grant update, delete on audit_log to %I', current_user);
end
$$;
-- +goose StatementEnd
drop table audit_pending;
drop table audit_chain_head;
drop table audit_log;
