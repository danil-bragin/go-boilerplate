-- Initialisation script executed once when the postgres container is first created.
-- Creates one database per service and a shared application role.
--
-- Audit append-only model (round-8 B1, round-8 security review #2):
--   * `audit_admin` — a dedicated privileged role that OWNS audit_log (after
--                     the append-only migration transfers ownership to it) and
--                     retains UPDATE/DELETE. The retention cleaner connects as
--                     this role via PG_AUDIT_ADMIN_URL so age-based pruning
--                     works while the app's own connection cannot.
--   * `app`         — the application role services connect as. Owns the OTHER
--                     service tables, but on audit_log it is a NON-OWNER granted
--                     only INSERT + SELECT; UPDATE/DELETE are REVOKEd (see the
--                     append-only migration). Because it does not OWN audit_log,
--                     the REVOKE actually bites (a table owner keeps full rights
--                     implicitly — the gap this change closes).
--
-- OWNERSHIP TRANSFER MECHANICS: the append-only migration runs as `app` and
-- does `ALTER TABLE audit_log OWNER TO audit_admin`. ALTER ... OWNER requires
-- the executing role to be a MEMBER of the new owner, so we grant audit_admin
-- to app — but WITH INHERIT FALSE so app does NOT automatically inherit
-- audit_admin's UPDATE/DELETE in normal queries (the append-only boundary holds
-- for ordinary app traffic). RESIDUAL CAVEAT: a compromised app could still
-- `SET ROLE audit_admin` to escalate; the HARDENED deployment eliminates even
-- that by running migrations as audit_admin/a bootstrap role and NOT granting
-- audit_admin to app at all. The hash chain (audit.WithChainKey / AUDIT_CHAIN_KEY)
-- is the defence-in-depth that makes such an escalation tamper-EVIDENT.
-- append_only_test proves the INSERT/SELECT-only boundary from a non-owner role.

CREATE USER app WITH PASSWORD 'app';
CREATE USER audit_admin WITH PASSWORD 'audit_admin';

-- Let `app` hand audit_log ownership to audit_admin during migration without
-- inheriting its privileges in day-to-day queries (INHERIT FALSE).
GRANT audit_admin TO app WITH INHERIT FALSE;

CREATE DATABASE gateway    WITH OWNER app;
CREATE DATABASE orders     WITH OWNER app;
CREATE DATABASE payments   WITH OWNER app;
CREATE DATABASE notifications WITH OWNER app;

\connect gateway
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE gateway TO audit_admin;
GRANT USAGE, CREATE ON SCHEMA public TO audit_admin;

\connect orders
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE orders TO audit_admin;
GRANT USAGE, CREATE ON SCHEMA public TO audit_admin;

\connect payments
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE payments TO audit_admin;
GRANT USAGE, CREATE ON SCHEMA public TO audit_admin;

\connect notifications
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
GRANT CONNECT ON DATABASE notifications TO audit_admin;
GRANT USAGE, CREATE ON SCHEMA public TO audit_admin;
