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
