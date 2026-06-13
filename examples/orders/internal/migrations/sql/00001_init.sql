-- +goose Up
-- Collapsed baseline — never deployed; merged from the original chain.
-- DO NOT re-collapse a live schema.
--
-- Folds the original 6-file chain into one baseline:
--   00001_init                  orders / outbox (partitioned) / inbox / audit_log
--   00002_outbox_topic          outbox.topic (not null)
--   00003_payment_tracking      orders.payment_timeout_emitted + orders_unpaid_idx
--   00004_audit_chain_append_only  audit_log.prev_hash/entry_hash + audit_chain_head
--                               + append-only ownership-transfer do-block
--   00005_audit_chain_shards    audit_log.chain_id + audit_log_chain_idx
--                               + drop audit_chain_head singleton
--   00006_audit_pending         audit_pending (durable-audit staging, app-owned)

create table orders (
    id                      uuid        primary key,
    customer_id             text        not null,
    amount_cents            bigint      not null,
    currency                text        not null,
    status                  text        not null,
    created_at              timestamptz not null default now(),
    -- payment_timeout_emitted (was 00003): guards the unpaid-order watcher; the
    -- flag is set in the SAME transaction that enqueues the OrderPaymentTimedOut
    -- outbox row, making the emit a compare-and-set (at-most-once per order).
    payment_timeout_emitted boolean     not null default false
);

create index orders_unpaid_idx on orders (created_at)
    where status = 'created' and payment_timeout_emitted = false;

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

-- audit_log in FINAL shape: hash-chain cols (was 00004: prev_hash, entry_hash)
-- + chain_id (was 00005, default 1 → single global chain until sharded) folded
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

-- audit_chain_head (was 00004): genesis row; the 00004 singleton check was
-- dropped in 00005 (sharded chains hold more than one head row), so the
-- baseline never declares it.
create table audit_chain_head (
    id         smallint primary key default 1,
    last_hash  bytea       not null,
    updated_at timestamptz not null default now()
);

insert into audit_chain_head (id, last_hash)
values (1, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);

-- audit_pending (was 00006): durable-audit staging
-- (see docs/superpowers/specs/2026-06-13-durable-audit-design.md). A Durable
-- command inserts an audit INTENT here inside its own transaction (cheap, no
-- chain-head lock); a single-active per-shard worker drains these into the
-- append-only audit_log hash chain exactly-once. Owned by the app role: unlike
-- audit_log (append-only, audit_admin-owned), this is transient staging the app
-- must INSERT and the drain must DELETE.
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

-- Append-only ownership transfer (was 00004), preserved VERBATIM and run last
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
drop table audit_pending;
drop table audit_chain_head;
drop table audit_log;
drop table inbox;
drop index outbox_unpublished_idx;
drop table outbox;
drop index orders_unpaid_idx;
drop table orders;
