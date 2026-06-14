package order_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/examples/orders/internal/migrations"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRepo migrates a fresh database inside the package-shared Postgres
// container and returns the repository over it.
func newRepo(t *testing.T) (*order.PgRepository, *pg.Pool) {
	t.Helper()
	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return order.NewPgRepository(pool), pool
}

func insertCreated(t *testing.T, repo *order.PgRepository) uuid.UUID {
	t.Helper()
	id := uuid.New()
	require.NoError(t, repo.Insert(context.Background(), order.Order{
		ID: id, CustomerID: "cust-1", Amount: amt(1500, "USD"), Status: order.StatusCreated,
	}))
	return id
}

// TestPgRepository_InsertGet pins the row round-trip: DB-generated created_at
// (UTC instant) and field fidelity.
func TestPgRepository_InsertGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, _ := newRepo(t)
	id := insertCreated(t, repo)

	got, err := repo.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "cust-1", got.CustomerID)
	assert.True(t, got.Amount.Equal(amt(1500, "USD")), "amount round-trips as money (got %s)", got.Amount)
	assert.Equal(t, order.StatusCreated, got.Status)
	assert.False(t, got.CreatedAt.IsZero(), "created_at is DB time (DEFAULT now()), set without the app supplying it")
	assert.WithinDuration(t, time.Now().UTC(), got.CreatedAt, time.Minute)
}

// TestPgRepository_MarkPaymentOutcome_FirstOutcomeWins pins the guarded
// UPDATE: the first terminal outcome applies; the second (conflicting or
// duplicate) reports applied=false instead of overwriting.
func TestPgRepository_MarkPaymentOutcome_FirstOutcomeWins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, _ := newRepo(t)
	ctx := context.Background()
	id := insertCreated(t, repo)

	applied, err := repo.MarkPaymentOutcome(ctx, id, order.StatusPaid)
	require.NoError(t, err)
	assert.True(t, applied)

	applied, err = repo.MarkPaymentOutcome(ctx, id, order.StatusPaymentFailed)
	require.NoError(t, err)
	assert.False(t, applied, "a second outcome must not overwrite the first")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, order.StatusPaid, got.Status)

	// Unknown order: guard matches nothing.
	applied, err = repo.MarkPaymentOutcome(ctx, uuid.New(), order.StatusPaid)
	require.NoError(t, err)
	assert.False(t, applied)
}

// TestPgRepository_MarkTimeoutEmitted_CASClaim pins the compare-and-set: the
// first claim wins and flips the status; re-claims and post-payment claims
// lose.
func TestPgRepository_MarkTimeoutEmitted_CASClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, _ := newRepo(t)
	ctx := context.Background()

	id := insertCreated(t, repo)
	claimed, err := repo.MarkTimeoutEmitted(ctx, id)
	require.NoError(t, err)
	assert.True(t, claimed)

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, order.StatusPaymentTimeout, got.Status,
		"the claim must flip the status in the same statement so a late payment outcome is a no-op")

	claimed, err = repo.MarkTimeoutEmitted(ctx, id)
	require.NoError(t, err)
	assert.False(t, claimed, "re-poll must not claim twice")

	// A paid order can never be claimed.
	paidID := insertCreated(t, repo)
	_, err = repo.MarkPaymentOutcome(ctx, paidID, order.StatusPaid)
	require.NoError(t, err)
	claimed, err = repo.MarkTimeoutEmitted(ctx, paidID)
	require.NoError(t, err)
	assert.False(t, claimed)
}

// TestPgRepository_ListUnpaidExpired pins the DB-clock cutoff and the
// status/flag filters.
func TestPgRepository_ListUnpaidExpired(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, pool := newRepo(t)
	ctx := context.Background()

	expiredID := insertCreated(t, repo)
	insertCreated(t, repo) // fresh order: inside the window, must not match
	paidID := insertCreated(t, repo)

	// Age two rows past the window; the cutoff compares against the DB clock.
	for _, id := range []uuid.UUID{expiredID, paidID} {
		_, err := pool.Writer().Exec(ctx,
			`update orders set created_at = now() - interval '1 hour' where id = $1`, id)
		require.NoError(t, err)
	}
	_, err := repo.MarkPaymentOutcome(ctx, paidID, order.StatusPaid)
	require.NoError(t, err)

	rows, err := repo.ListUnpaidExpired(ctx, 15*time.Minute, 100)
	require.NoError(t, err)
	require.Len(t, rows, 1, "only the expired, still-'created' order qualifies")
	assert.Equal(t, expiredID, rows[0].ID)
	assert.False(t, rows[0].CreatedAt.IsZero())
}

// TestPgRepository_JoinsAmbientTransaction pins the ambient-tx invariant the
// repository documents: inside pg.RunInTx every write joins the caller's
// transaction, so a rollback discards them all (and an outbox enqueue in the
// same ctx would roll back with them).
func TestPgRepository_JoinsAmbientTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, pool := newRepo(t)
	ctx := context.Background()
	id := uuid.New()

	rollback := assert.AnError
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		if err := repo.Insert(ctx, order.Order{
			ID: id, CustomerID: "c", Amount: amt(1, "USD"), Status: order.StatusCreated,
		}); err != nil {
			return err
		}
		// Visible inside the transaction (read-your-writes via FromContext).
		got, err := repo.Get(ctx, id)
		if err != nil {
			return err
		}
		assert.Equal(t, id, got.ID)
		return rollback
	})
	require.ErrorIs(t, err, rollback)

	_, err = repo.Get(ctx, id)
	require.Error(t, err, "rolled-back insert must not be visible outside the transaction")
}
