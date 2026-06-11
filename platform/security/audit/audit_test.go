package audit_test

import (
	"context"
	"embed"
	"errors"
	"testing"
	"time"

	"go-boilerplate/platform/config"
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
	pool, _ := newPoolDSN(t)
	return pool
}

// newPoolDSN is newPool but also returns the DSN, so tests that need a second
// (privileged) connection — e.g. the cleanup admin pool — can dial it.
func newPoolDSN(t *testing.T) (*pg.Pool, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool, dsn
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

// seedAuditRow inserts one audit_log row with an explicit created_at so the
// since/order assertions are deterministic.
func seedAuditRow(t *testing.T, pool *pg.Pool, actor, action, subject string, meta string, at time.Time) {
	t.Helper()
	_, err := pool.Writer().Exec(context.Background(),
		`insert into audit_log (actor, action, subject, metadata, created_at) values ($1, $2, $3, $4, $5)`,
		actor, action, subject, meta, at)
	require.NoError(t, err)
}

// TestPgStore_QueryByActor: the DSAR read path — one actor's entries, newest
// first, since-inclusive filtering, limit, actor isolation, and metadata
// round-trip.
func TestPgStore_QueryByActor(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedAuditRow(t, pool, "alice", "order:create", "order-1", `{"ip":"10.0.0.1"}`, base)
	seedAuditRow(t, pool, "alice", "order:create", "order-2", `{}`, base.Add(1*time.Hour))
	seedAuditRow(t, pool, "alice", "payment:process", "pay-1", `{}`, base.Add(2*time.Hour))
	seedAuditRow(t, pool, "bob", "order:create", "order-9", `{}`, base.Add(90*time.Minute))

	// All of alice's entries, newest first; bob's are invisible.
	entries, err := store.Query(ctx, "alice", time.Time{}, 10)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, []string{"pay-1", "order-2", "order-1"},
		[]string{entries[0].Subject, entries[1].Subject, entries[2].Subject},
		"entries must be ordered created_at DESC")
	for _, e := range entries {
		require.Equal(t, "alice", e.Actor)
	}
	require.Equal(t, map[string]string{"ip": "10.0.0.1"}, entries[2].Metadata,
		"metadata must round-trip")
	require.True(t, entries[2].At.Equal(base), "At must carry created_at")

	// since is inclusive: from base+1h on, two entries remain.
	entries, err = store.Query(ctx, "alice", base.Add(1*time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "pay-1", entries[0].Subject)
	require.Equal(t, "order-2", entries[1].Subject)

	// limit keeps the NEWEST rows.
	entries, err = store.Query(ctx, "alice", time.Time{}, 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "pay-1", entries[0].Subject)

	// Unknown actor: empty, no error.
	entries, err = store.Query(ctx, "nobody", time.Time{}, 10)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestRecordOutOfBand_DenialStormBounded proves the denial-audit DoS bound
// (security review #5): a burst of denial audits for one (actor,action) writes
// only up to the per-key burst depth; the rest are coalesced (onDenialDropped
// fires) instead of each taking the global chain lock. Non-denial out-of-band
// writes are unaffected.
func TestRecordOutOfBand_DenialStormBounded(t *testing.T) {
	pool := newPool(t)
	var dropped int
	store := audit.NewPgStore(pool, audit.WithOnDenialDropped(func() { dropped++ }))
	ctx := context.Background()

	const burst = 50
	for i := 0; i < burst; i++ {
		err := store.RecordOutOfBand(ctx, audit.Entry{
			Actor:   "attacker",
			Action:  audit.ActionAuthzDenied,
			Subject: "forbidden-endpoint",
		})
		require.NoError(t, err)
	}

	// Count the denial rows actually written.
	var written int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where action = $1 and actor = 'attacker'`,
		audit.ActionAuthzDenied).Scan(&written))

	require.LessOrEqual(t, written, 5, "denial storm must be bounded to the per-key burst")
	require.Greater(t, written, 0, "the onset of the abuse pattern must still be recorded")
	require.Equal(t, burst-written, dropped, "every coalesced denial must fire onDenialDropped")

	// A denial for a DIFFERENT actor is independently admitted.
	require.NoError(t, store.RecordOutOfBand(ctx, audit.Entry{
		Actor:   "legit-user",
		Action:  audit.ActionAuthzDenied,
		Subject: "forbidden-endpoint",
	}))
	var legit int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from audit_log where action = $1 and actor = 'legit-user'`,
		audit.ActionAuthzDenied).Scan(&legit))
	require.Equal(t, 1, legit, "a different principal's denial is not silenced by the attacker's flood")
}
