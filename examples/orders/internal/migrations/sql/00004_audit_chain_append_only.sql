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
-- UPDATE/DELETE. Retention runs through the privileged audit_admin role
-- (PG_AUDIT_ADMIN_URL); see deploy/postgres/init.sql.
-- +goose StatementBegin
do $$
begin
    execute format('revoke update, delete on audit_log from %I', current_user);
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        execute 'grant select, delete on audit_log to audit_admin';
    end if;
end
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
do $$
begin
    execute format('grant update, delete on audit_log to %I', current_user);
end
$$;
-- +goose StatementEnd
drop table audit_chain_head;
alter table audit_log
    drop column entry_hash,
    drop column prev_hash;
