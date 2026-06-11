-- +goose Up
-- Append-only audit_log: the application role may INSERT and SELECT but never
-- UPDATE or DELETE. This makes the table tamper-resistant at the database
-- privilege boundary — even a fully compromised app connection cannot rewrite
-- or erase history through the ORM/SQL path.
--
-- The role name is resolved at migration time to current_user (the role the
-- app connects as). deploy/postgres/init.sql provisions a dedicated
-- "audit_admin" role that retains DELETE for retention; cleanup dials it via
-- PG_AUDIT_ADMIN_URL (see cleanup.go). When PG_AUDIT_ADMIN_URL is unset the
-- retention DELETE is blocked by this REVOKE and Cleanup skips with a WARN.
--
-- +goose StatementBegin
do $$
begin
    execute format('revoke update, delete on audit_log from %I', current_user);
    -- Grant the privileged retention role DELETE + SELECT when it exists
    -- (deploy/postgres/init.sql provisions "audit_admin"). Skipped silently in
    -- environments that run a single role (tests, local dev without the role).
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
