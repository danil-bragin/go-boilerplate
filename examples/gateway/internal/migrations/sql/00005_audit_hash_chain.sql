-- +goose Up
-- Hash-chain tamper-evidence for the gateway audit_log (mirrors the platform
-- audit migration 00004_audit_hash_chain). See audit.Record / audit.VerifyChain
-- for the chaining scheme. The gateway records audit entries on admin DSAR
-- reads and access denials (round-8 B3), so it carries the same chain head.
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

-- app reads + advances (and lazily inserts sharded — ADR-0018) chain-head rows;
-- grant to the DATABASE OWNER (runtime app role) so it works under a privileged/
-- superuser migrate role (PG_MIGRATE_URL) too, not just migrate-as-app.
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

-- +goose Down
drop table audit_chain_head;
alter table audit_log
    drop column entry_hash,
    drop column prev_hash;
