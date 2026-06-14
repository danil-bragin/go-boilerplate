-- +goose Up
-- Collapsed baseline — never deployed; merged from the original chain.
-- DO NOT re-collapse a live schema.
--
-- Folds the original 4-file chain into one baseline:
--   00001_init                     payments / outbox (partitioned) / inbox / audit_log
--   00002_outbox_topic             outbox.topic (not null)
--   00003_audit_chain_append_only  audit_log.prev_hash/entry_hash + audit_chain_head
--                                  + append-only ownership-transfer do-block
--   00004_audit_chain_shards       audit_log.chain_id + audit_log_chain_idx
--                                  + drop audit_chain_head singleton

-- Money is stored as the canonical platform/money two-column shape: amount
-- NUMERIC (arbitrary precision/scale, lossless — never bigint cents, never PG
-- money/float) + asset TEXT. This generalizes the payments service from
-- fiat-2dp-only cents to any asset (crypto/FX). See ADR-0020.
create table payments (
    id         uuid        primary key,
    order_id   text        not null,
    amount     numeric     not null,
    asset      text        not null,
    status     text        not null,
    created_at timestamptz not null default now()
);

-- Partitioned by created_at from day one (ADR-0016); the DEFAULT partition
-- keeps "simple" mode behaving exactly like a plain table.
-- topic (was 00002): explicit destination topic per row; aggregate_type stays
-- the aggregate kind.
create table outbox (
    id             uuid        not null,
    aggregate_type text        not null,
    aggregate_id   text        not null,
    event_type     text        not null,
    payload        bytea       not null,
    headers        jsonb       not null default '{}'::jsonb,
    topic          text        not null,
    created_at     timestamptz not null default now(),
    published_at   timestamptz,
    primary key (id, created_at)
) partition by range (created_at);

create table outbox_default partition of outbox default;

create index outbox_unpublished_idx on outbox (created_at) where published_at is null;

create table inbox (
    consumer     text        not null,
    message_id   text        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer, message_id)
);

-- audit_log in FINAL shape: hash-chain cols (was 00003: prev_hash, entry_hash)
-- + chain_id (was 00004, default 1 → single global chain until sharded) folded
-- into the create.
create table audit_log (
    id         bigserial   primary key,
    actor      text        not null,
    action     text        not null,
    subject    text        not null,
    metadata   jsonb       not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    prev_hash  bytea,
    entry_hash bytea,
    chain_id   smallint    not null default 1
);

create index audit_log_action_idx on audit_log (action, created_at);
create index audit_log_chain_idx on audit_log (chain_id, id);

-- audit_chain_head (was 00003): genesis row; the 00003 singleton check was
-- dropped in 00004 (sharded chains hold more than one head row), so the
-- baseline never declares it.
create table audit_chain_head (
    id         smallint primary key default 1,
    last_hash  bytea       not null,
    updated_at timestamptz not null default now()
);

insert into audit_chain_head (id, last_hash)
values (1, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);

-- Append-only ownership transfer (was 00003), preserved VERBATIM and run last
-- so audit_log already exists. The app role may INSERT + SELECT audit_log but
-- never UPDATE/DELETE. OWNERSHIP IS LOAD-BEARING — an OWNER keeps UPDATE/DELETE
-- implicitly, so unless audit_log is owned by a NON-app role the REVOKE is a
-- no-op. When audit_admin exists (deploy/postgres/init.sql) transfer ownership
-- of the table + its bigserial sequence to it and re-grant the app role only
-- INSERT + SELECT; the REVOKE then actually denies the app's UPDATE/DELETE.
-- Retention runs through audit_admin (PG_AUDIT_ADMIN_URL). See the platform
-- audit migration 00003 for the full rationale and trade-off.
-- +goose StatementBegin
-- Grant/revoke against the DATABASE OWNER (the runtime app role), not
-- current_user, so this works whether migrations run as app or a privileged/
-- superuser migrate role (PG_MIGRATE_URL). See platform audit 00003.
do $$
declare
    app_role text := (select pg_catalog.pg_get_userbyid(datdba)
                      from pg_database where datname = current_database());
begin
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        execute 'alter table audit_log owner to audit_admin';
        -- Act AS the new owner — app is a non-owner after the transfer.
        set local role audit_admin;
        execute format('grant select, insert on audit_log to %I', app_role);
        execute format('grant usage, select on sequence audit_log_id_seq to %I', app_role);
        execute format('revoke update, delete on audit_log from %I', app_role);
        reset role;
    else
        execute format('revoke update, delete on audit_log from %I', app_role);
    end if;
    -- app reads + advances (and lazily inserts sharded) chain-head rows; app owns
    -- audit_chain_head under migrate-as-app, so this is a self-grant no-op there.
    execute format('grant select, insert, update on audit_chain_head to %I', app_role);
end
$$;
-- +goose StatementEnd

-- +goose Down
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
drop table audit_chain_head;
drop table audit_log;
drop table inbox;
drop index outbox_unpublished_idx;
drop table outbox;
drop table payments;
