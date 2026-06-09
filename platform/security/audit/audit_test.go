package audit_test

import (
	"context"
	"embed"
	"errors"
	"testing"
	"time"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

//go:embed migrations/*.sql
var migrations embed.FS

// newPool starts a Postgres container, runs audit migrations, and returns a
// ready-to-use pool. The pool is closed when t finishes.
func newPool(t *testing.T) *pg.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

// auditRow represents one audit_log row as read back in tests.
type auditRow struct {
	Actor   string
	Action  string
	Subject string
	At      time.Time
}

// readAuditRows returns all rows from audit_log ordered by id.
func readAuditRows(t *testing.T, pool *pg.Pool) []auditRow {
	t.Helper()
	rows, err := pool.Reader().Query(context.Background(),
		`select actor, action, subject, created_at from audit_log order by id`)
	require.NoError(t, err)
	defer rows.Close()

	var out []auditRow
	for rows.Next() {
		var r auditRow
		require.NoError(t, rows.Scan(&r.Actor, &r.Action, &r.Subject, &r.At))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// businessRows counts rows in the business_write table.
func businessRows(t *testing.T, pool *pg.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select count(*) from business_write`).Scan(&n))
	return n
}

// createBusinessTable creates a scratch table used by tests to verify that
// business writes roll back together with audit writes.
func createBusinessTable(t *testing.T, pool *pg.Pool) {
	t.Helper()
	_, err := pool.Writer().Exec(context.Background(),
		`create table business_write (id text not null)`)
	require.NoError(t, err)
}

// --- command/result types used across tests ---

type orderCmd struct {
	OrderID string
}
type orderResult struct{}

// --- tests ---

// TestAudit_RecordsOnSuccessWithinTx verifies that a handler wrapped with
// Transaction+Audit records one audit row on success and that the actor is
// taken from the ctx principal.
func TestAudit_RecordsOnSuccessWithinTx(t *testing.T) {
	pool := newPool(t)
	createBusinessTable(t, pool)
	ctx := context.Background()

	store := audit.NewPgStore(pool)

	handler := cqrs.HandlerFunc[orderCmd, orderResult](func(ctx context.Context, cmd orderCmd) (orderResult, error) {
		_, err := pg.FromContext(ctx, pool).Exec(ctx,
			`insert into business_write (id) values ($1)`, cmd.OrderID)
		return orderResult{}, err
	})

	decorated := cqrs.Decorate(
		handler,
		cqrs.Transaction[orderCmd, orderResult](pool),
		audit.Audit[orderCmd, orderResult](store, "order:create", func(c orderCmd) string { return c.OrderID }),
	)

	ctx = auth.Into(ctx, auth.Principal{Subject: "u1"})
	_, err := decorated(ctx, orderCmd{OrderID: "order-42"})
	require.NoError(t, err)

	// Check business write committed.
	require.Equal(t, 1, businessRows(t, pool), "business write must be committed")

	// Check audit row.
	rows := readAuditRows(t, pool)
	require.Len(t, rows, 1)
	require.Equal(t, "u1", rows[0].Actor)
	require.Equal(t, "order:create", rows[0].Action)
	require.Equal(t, "order-42", rows[0].Subject)
}

// TestAudit_RollsBackWithCommandError verifies that when the handler returns
// an error the audit entry is NOT written and the tx is rolled back.
func TestAudit_RollsBackWithCommandError(t *testing.T) {
	pool := newPool(t)
	createBusinessTable(t, pool)
	ctx := context.Background()

	store := audit.NewPgStore(pool)
	errBoom := errors.New("boom")

	handler := cqrs.HandlerFunc[orderCmd, orderResult](func(ctx context.Context, cmd orderCmd) (orderResult, error) {
		// Do a partial write, then fail.
		_, _ = pg.FromContext(ctx, pool).Exec(ctx,
			`insert into business_write (id) values ($1)`, cmd.OrderID)
		return orderResult{}, errBoom
	})

	decorated := cqrs.Decorate(
		handler,
		cqrs.Transaction[orderCmd, orderResult](pool),
		audit.Audit[orderCmd, orderResult](store, "order:create", func(c orderCmd) string { return c.OrderID }),
	)

	ctx = auth.Into(ctx, auth.Principal{Subject: "u1"})
	_, err := decorated(ctx, orderCmd{OrderID: "order-99"})
	require.ErrorIs(t, err, errBoom)

	// No business write must be present (tx rolled back).
	require.Equal(t, 0, businessRows(t, pool), "business write must be rolled back")
	// No audit row either.
	require.Empty(t, readAuditRows(t, pool), "audit row must not be written on command error")
}

// errStore is a fake Store whose Record always returns an error.
type errStore struct{ err error }

func (s *errStore) Record(_ context.Context, _ audit.Entry) error { return s.err }

// TestAudit_StoreErrorFailsCommand verifies that when Record returns an error
// on a SUCCESSFUL command, the behavior returns an error AND the business write
// is rolled back (cannot-audit → do not commit).
func TestAudit_StoreErrorFailsCommand(t *testing.T) {
	pool := newPool(t)
	createBusinessTable(t, pool)
	ctx := context.Background()

	errAudit := errors.New("audit store unavailable")
	store := &errStore{err: errAudit}

	handler := cqrs.HandlerFunc[orderCmd, orderResult](func(ctx context.Context, cmd orderCmd) (orderResult, error) {
		_, err := pg.FromContext(ctx, pool).Exec(ctx,
			`insert into business_write (id) values ($1)`, cmd.OrderID)
		return orderResult{}, err
	})

	decorated := cqrs.Decorate(
		handler,
		cqrs.Transaction[orderCmd, orderResult](pool),
		audit.Audit[orderCmd, orderResult](store, "order:create", func(c orderCmd) string { return c.OrderID }),
	)

	ctx = auth.Into(ctx, auth.Principal{Subject: "u1"})
	_, err := decorated(ctx, orderCmd{OrderID: "order-77"})
	require.ErrorIs(t, err, errAudit, "audit store error must be returned")

	// Business write must be rolled back because the audit failed.
	require.Equal(t, 0, businessRows(t, pool),
		"business write must be rolled back when audit cannot be written")
}

// TestAudit_AnonymousWhenNoPrincipal verifies that when no principal is in the
// context the actor field is set to "anonymous".
func TestAudit_AnonymousWhenNoPrincipal(t *testing.T) {
	pool := newPool(t)
	createBusinessTable(t, pool)
	// No principal stored in ctx.
	ctx := context.Background()

	store := audit.NewPgStore(pool)

	handler := cqrs.HandlerFunc[orderCmd, orderResult](func(_ context.Context, _ orderCmd) (orderResult, error) {
		return orderResult{}, nil
	})

	decorated := cqrs.Decorate(
		handler,
		cqrs.Transaction[orderCmd, orderResult](pool),
		audit.Audit[orderCmd, orderResult](store, "order:create", func(c orderCmd) string { return c.OrderID }),
	)

	_, err := decorated(ctx, orderCmd{OrderID: "order-anon"})
	require.NoError(t, err)

	rows := readAuditRows(t, pool)
	require.Len(t, rows, 1)
	require.Equal(t, "anonymous", rows[0].Actor)
}
