-- +goose Up
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
-- +goose StatementBegin
do $$
-- Grant/revoke against the DATABASE OWNER (the runtime app role —
-- init.sql: CREATE DATABASE <svc> WITH OWNER app), NOT current_user. This makes
-- the migration correct whether it runs AS the app role or as a privileged /
-- superuser migrate role (PG_MIGRATE_URL): the audit privileges always land on
-- the role the service actually connects as, instead of leaking onto the
-- migrate role.
declare
    app_role text := (select pg_catalog.pg_get_userbyid(datdba)
                      from pg_database where datname = current_database());
begin
    if exists (select 1 from pg_roles where rolname = 'audit_admin') then
        -- Hand the table + its owned sequence to the privileged role so the app
        -- role becomes a non-owner the REVOKE can actually constrain.
        execute 'alter table audit_log owner to audit_admin';
        execute format('grant select, insert on audit_log to %I', app_role);
        execute format('grant usage, select on sequence audit_log_id_seq to %I', app_role);
        execute 'grant select, delete on audit_log to audit_admin';
    end if;
    -- REVOKE UPDATE/DELETE from the app role. Bites when the app is a non-owner
    -- (audit_admin branch ran); a no-op for an owner/superuser.
    execute format('revoke update, delete on audit_log from %I', app_role);
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
