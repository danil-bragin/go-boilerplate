package audit_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// restrictedDSN returns a DSN for a freshly-created, NON-OWNER login role that
// holds only INSERT + SELECT on audit_log (the append-only app role). The
// owner role (pgtest's default) always keeps full rights — Postgres owner
// privileges are implicit and cannot be revoked — so the privilege boundary
// can only be proven from a separate, non-owner role.
//
// The role is created against the same database the pool is connected to. The
// app role from migration 00003's REVOKE is current_user at migration time,
// i.e. the owner; here we additionally model the deployed split (init.sql) by
// granting the new role INSERT+SELECT and explicitly withholding UPDATE/DELETE.
func restrictedDSN(t *testing.T, pool *pg.Pool, baseDSN string) string {
	t.Helper()
	ctx := context.Background()
	const role = "audit_app_restricted"
	const pw = "restricted"
	stmts := []string{
		`drop role if exists ` + role,
		`create role ` + role + ` login password '` + pw + `'`,
		`grant insert, select on audit_log to ` + role,
		`grant usage, select on sequence audit_log_id_seq to ` + role,
		`revoke update, delete on audit_log from ` + role,
	}
	for _, s := range stmts {
		_, err := pool.Writer().Exec(ctx, s)
		require.NoError(t, err, "setup restricted role: %s", s)
	}
	// Rewrite the DSN's userinfo to the restricted role.
	return rewriteUser(t, baseDSN, role, pw)
}

// TestAuditAppendOnly_RestrictedRoleCannotMutate proves the privilege boundary:
// the append-only app role may INSERT and SELECT audit_log but UPDATE and
// DELETE are denied at the database layer. This is the tamper-resistance B1
// ships — a compromised app connection cannot rewrite or erase history.
func TestAuditAppendOnly_RestrictedRoleCannotMutate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	baseDSN := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, baseDSN, migrations, "migrations"))

	ownerPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(baseDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ownerPool.Close(ctx) })

	restDSN := restrictedDSN(t, ownerPool, baseDSN)
	restConn, err := pgxpool.New(ctx, restDSN)
	require.NoError(t, err)
	t.Cleanup(restConn.Close)

	// INSERT is allowed.
	_, err = restConn.Exec(ctx,
		`insert into audit_log (actor, action, subject) values ('u1', 'order:create', 'order-1')`)
	require.NoError(t, err, "restricted role must be able to INSERT")

	// SELECT is allowed.
	var n int
	require.NoError(t, restConn.QueryRow(ctx, `select count(*) from audit_log`).Scan(&n))
	require.Equal(t, 1, n)

	// UPDATE is denied.
	_, err = restConn.Exec(ctx, `update audit_log set actor = 'evil' where subject = 'order-1'`)
	require.Error(t, err, "restricted role must NOT be able to UPDATE audit_log")
	require.Contains(t, err.Error(), "permission denied")

	// DELETE is denied.
	_, err = restConn.Exec(ctx, `delete from audit_log where subject = 'order-1'`)
	require.Error(t, err, "restricted role must NOT be able to DELETE audit_log")
	require.Contains(t, err.Error(), "permission denied")
}

// TestAuditAppendOnly_OwnershipTransferModel proves the DEPLOYED ownership
// model (security review #2a): when audit_log is OWNED BY audit_admin and the
// app role holds only INSERT+SELECT, the append-only REVOKE genuinely bites for
// the app. This is the boundary the boilerplate ships once the append-only
// migration transfers ownership to audit_admin — verified here against a
// non-owner, non-superuser role that stands in for the production app role
// (the pgtest default app role is a SUPERUSER and would bypass the check, which
// is exactly why ownership/role separation is load-bearing).
func TestAuditAppendOnly_OwnershipTransferModel(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	baseDSN := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, baseDSN, migrations, "migrations"))

	ownerPool, err := pg.New(ctx, pg.Config{DSN: config.Secret(baseDSN)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ownerPool.Close(ctx) })

	const (
		adminRole = "audit_admin"
		adminPw   = "audit_admin"
		appRole   = "deployed_app"
		appPw     = "deployed_app"
	)
	// Provision the deployed split: audit_admin OWNS audit_log; the (non-owner,
	// non-superuser) app role gets INSERT+SELECT only and UPDATE/DELETE revoked.
	stmts := []string{
		`drop role if exists ` + appRole,
		`drop role if exists ` + adminRole,
		`create role ` + adminRole + ` login password '` + adminPw + `'`,
		`create role ` + appRole + ` login password '` + appPw + `'`,
		`grant usage on schema public to ` + adminRole,
		`grant usage on schema public to ` + appRole,
		`alter table audit_log owner to ` + adminRole,
		`grant select, insert on audit_log to ` + appRole,
		`grant usage, select on sequence audit_log_id_seq to ` + appRole,
		`revoke update, delete on audit_log from ` + appRole,
		`grant select, delete on audit_log to ` + adminRole,
	}
	for _, s := range stmts {
		_, err := ownerPool.Writer().Exec(ctx, s)
		require.NoError(t, err, "setup: %s", s)
	}

	// App role (non-owner): INSERT + SELECT allowed, UPDATE + DELETE denied.
	appConn, err := pgxpool.New(ctx, rewriteUser(t, baseDSN, appRole, appPw))
	require.NoError(t, err)
	t.Cleanup(appConn.Close)

	_, err = appConn.Exec(ctx,
		`insert into audit_log (actor, action, subject) values ('u1','order:create','order-1')`)
	require.NoError(t, err, "app role must INSERT")
	var n int
	require.NoError(t, appConn.QueryRow(ctx, `select count(*) from audit_log`).Scan(&n))
	require.Equal(t, 1, n, "app role must SELECT")

	_, err = appConn.Exec(ctx, `update audit_log set actor='evil' where subject='order-1'`)
	require.Error(t, err, "app role (non-owner) must NOT UPDATE")
	require.Contains(t, err.Error(), "permission denied")
	_, err = appConn.Exec(ctx, `delete from audit_log where subject='order-1'`)
	require.Error(t, err, "app role (non-owner) must NOT DELETE")
	require.Contains(t, err.Error(), "permission denied")

	// audit_admin (the owner / retention role) CAN delete — the cleanup path.
	adminConn, err := pgxpool.New(ctx, rewriteUser(t, baseDSN, adminRole, adminPw))
	require.NoError(t, err)
	t.Cleanup(adminConn.Close)
	tag, err := adminConn.Exec(ctx, `delete from audit_log where subject='order-1'`)
	require.NoError(t, err, "audit_admin (owner) must retain DELETE for retention")
	require.EqualValues(t, 1, tag.RowsAffected())
}

// TestAuditMigration_TransfersOwnershipWhenAuditAdminExists exercises the
// ownership-transfer BRANCH of the append-only migration: when an audit_admin
// role exists at migration time, migration 00003 must ALTER TABLE audit_log
// OWNER TO audit_admin (re-granting the app role INSERT+SELECT) without error,
// and audit_admin must end up the owner. This is the production path
// (deploy/postgres/init.sql provisions audit_admin before migrations run); the
// other append-only tests run as a single superuser where the branch is
// skipped, so this is the only coverage of the transfer SQL itself.
func TestAuditMigration_TransfersOwnershipWhenAuditAdminExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()

	// Provision audit_admin BEFORE migrating so the transfer branch fires.
	bootstrap, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	_, err = bootstrap.Exec(ctx, `create role audit_admin login password 'audit_admin'`)
	require.NoError(t, err)
	bootstrap.Close()

	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	var owner string
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select pg_get_userbyid(relowner) from pg_class where relname = 'audit_log'`).Scan(&owner))
	require.Equal(t, "audit_admin", owner, "migration must transfer audit_log ownership to audit_admin")
}

// TestAuditCleanup_DisabledWithoutAdminPool: with no admin pool wired, Cleanup
// is a deliberate no-op (ErrCleanupDisabled) — the append-only REVOKE means a
// DELETE through the app pool would be denied, so cleanup refuses rather than
// erroring per-tick on a permission failure.
func TestAuditCleanup_DisabledWithoutAdminPool(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	deleted, err := store.Cleanup(context.Background(), 30*24*time.Hour)
	require.ErrorIs(t, err, audit.ErrCleanupDisabled)
	require.Zero(t, deleted)
}
