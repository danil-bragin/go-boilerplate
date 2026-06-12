-- +goose Up
-- Audit hardening (round-8 lane B): append-only role + hash-chain tamper
-- evidence for this service's audit_log. Mirrors the platform audit migrations
-- 00003_audit_append_only + 00004_audit_hash_chain (the audit DDL is inlined
-- per service in 00001_init, so the hardening must be applied per service too).
alter table audit_log
    add column prev_hash  bytea,
    add column entry_hash bytea;

create table audit_chain_head (
    id         smallint primary key default 1,
    last_hash  bytea       not null,
    updated_at timestamptz not null default now(),
    constraint audit_chain_head_singleton check (id = 1)
);

insert into audit_chain_head (id, last_hash)
values (1, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);

-- Append-only: the app role may INSERT + SELECT audit_log but never
-- UPDATE/DELETE. OWNERSHIP IS LOAD-BEARING — an OWNER keeps UPDATE/DELETE
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
        execute format('grant select, insert on audit_log to %I', app_role);
        execute format('grant usage, select on sequence audit_log_id_seq to %I', app_role);
        execute 'grant select, delete on audit_log to audit_admin';
    end if;
    execute format('revoke update, delete on audit_log from %I', app_role);
    -- app reads + advances (and lazily inserts sharded) chain-head rows.
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
alter table audit_log
    drop column entry_hash,
    drop column prev_hash;
