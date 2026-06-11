package audit_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"go-boilerplate/platform/security/audit"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// rewriteUser returns dsn with its userinfo replaced by user:password — used
// to dial Postgres as a different role than the pgtest default.
func rewriteUser(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}

// adminPoolForCleanup wires a privileged pool on the store so Cleanup can run
// the retention DELETE. The pgtest owner role retains DELETE (owner privileges
// are implicit), so dialing the same DSN models the deployed audit_admin role.
func adminPoolForCleanup(t *testing.T, store *audit.PgStore, dsn string) {
	t.Helper()
	p, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	store.SetAdminPool(p)
}

// TestAuditCleanup_DeletesOldRowsOnly verifies that Cleanup on PgStore removes
// audit_log rows older than the retention window and keeps recent ones.
func TestAuditCleanup_DeletesOldRowsOnly(t *testing.T) {
	pool, dsn := newPoolDSN(t)
	ctx := context.Background()

	twoMonthsAgo := time.Now().UTC().Add(-60 * 24 * time.Hour)
	oneHourAgo := time.Now().UTC().Add(-1 * time.Hour)

	// Insert an old audit row.
	_, err := pool.Writer().Exec(ctx,
		`insert into audit_log (actor, action, subject, created_at) values ($1, $2, $3, $4)`,
		"u1", "order:create", "old-subject", twoMonthsAgo)
	require.NoError(t, err)

	// Insert a recent audit row.
	_, err = pool.Writer().Exec(ctx,
		`insert into audit_log (actor, action, subject, created_at) values ($1, $2, $3, $4)`,
		"u2", "order:create", "recent-subject", oneHourAgo)
	require.NoError(t, err)

	store := audit.NewPgStore(pool)
	adminPoolForCleanup(t, store, dsn)
	// Retention: 30 days → the 60-day-old row should be deleted; the 1h-old row kept.
	deleted, err := store.Cleanup(ctx, 30*24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "only the old audit row should be deleted")

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where subject = 'old-subject'`).Scan(&count))
	require.Equal(t, 0, count, "old audit row must be deleted")

	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where subject = 'recent-subject'`).Scan(&count))
	require.Equal(t, 1, count, "recent audit row must remain")
}

// TestAuditRunCleanup_TickerLoopCancels verifies RunCleanup exits on context cancellation.
func TestAuditRunCleanup_TickerLoopCancels(t *testing.T) {
	pool := newPool(t)

	store := audit.NewPgStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- store.RunCleanup(ctx, 20*time.Millisecond, 90*24*time.Hour)
	}()

	err := <-done
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"RunCleanup must return ctx.Err() on cancellation")
}
