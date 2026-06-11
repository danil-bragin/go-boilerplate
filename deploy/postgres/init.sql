-- Initialisation script executed once when the postgres container is first created.
-- Creates one database per service and a shared application role.
--
-- Audit append-only model (round-8 B1):
--   * `app`         — the application role services connect as. Owns the
--                     service tables. On audit_log it may INSERT + SELECT but
--                     NOT UPDATE/DELETE (see migration 00003_audit_append_only,
--                     which REVOKEs those from the app role per database).
--   * `audit_admin` — a dedicated privileged role that retains DELETE on
--                     audit_log. The retention cleaner connects as this role
--                     via PG_AUDIT_ADMIN_URL so age-based pruning works while
--                     the app's own connection cannot rewrite history.
--
-- NOTE: `app` owns the audit_log table, and a table OWNER keeps full rights
-- implicitly (REVOKE cannot strip owner privileges). For the REVOKE to bite in
-- a hardened deployment, run migrations as a bootstrap/owner role and connect
-- the application as a SEPARATE non-owner role granted only INSERT+SELECT. The
-- boilerplate keeps a single `app` role for simplicity; the migration + this
-- file document the split and the append_only_test proves the boundary from a
-- non-owner role.

CREATE USER app WITH PASSWORD 'app';
CREATE USER audit_admin WITH PASSWORD 'audit_admin';

CREATE DATABASE gateway    WITH OWNER app;
CREATE DATABASE orders     WITH OWNER app;
CREATE DATABASE payments   WITH OWNER app;
CREATE DATABASE notifications WITH OWNER app;

\connect gateway
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE gateway TO audit_admin;
GRANT USAGE ON SCHEMA public TO audit_admin;

\connect orders
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE orders TO audit_admin;
GRANT USAGE ON SCHEMA public TO audit_admin;

\connect payments
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE payments TO audit_admin;
GRANT USAGE ON SCHEMA public TO audit_admin;

\connect notifications
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE notifications TO audit_admin;
GRANT USAGE ON SCHEMA public TO audit_admin;
