package payment_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/examples/payments/internal/domain/payment"
	"go-boilerplate/examples/payments/internal/migrations"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRepo migrates a fresh database inside the package-shared Postgres
// container and returns the repository over it.
func newRepo(t *testing.T) (*payment.PgRepository, *pg.Pool) {
	t.Helper()
	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return payment.NewPgRepository(pool), pool
}

// TestPgRepository_InsertGetByOrder pins the row round-trip and the
// DB-generated created_at.
func TestPgRepository_InsertGetByOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, _ := newRepo(t)
	ctx := context.Background()

	id := uuid.New()
	orderID := uuid.NewString()
	require.NoError(t, repo.Insert(ctx, payment.Payment{
		ID: id, OrderID: orderID, Amount: usd(4200), Status: payment.StatusProcessed,
	}))

	got, err := repo.GetByOrder(ctx, orderID)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, orderID, got.OrderID)
	assert.True(t, got.Amount.Equal(usd(4200)), "amount round-trips as money (got %s)", got.Amount)
	assert.Equal(t, payment.StatusProcessed, got.Status)
	assert.False(t, got.CreatedAt.IsZero(), "created_at is DB time (DEFAULT now())")
	assert.WithinDuration(t, time.Now().UTC(), got.CreatedAt, time.Minute)

	_, err = repo.GetByOrder(ctx, uuid.NewString())
	require.Error(t, err, "unknown order has no payment")
}

// TestPgRepository_JoinsAmbientTransaction pins the ambient-tx invariant: a
// rollback discards the insert (and would discard the outbox enqueue sharing
// the same ctx).
func TestPgRepository_JoinsAmbientTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	repo, pool := newRepo(t)
	ctx := context.Background()
	orderID := uuid.NewString()

	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		if err := repo.Insert(ctx, payment.Payment{
			ID: uuid.New(), OrderID: orderID, Amount: usd(1), Status: payment.StatusProcessed,
		}); err != nil {
			return err
		}
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	_, err = repo.GetByOrder(ctx, orderID)
	require.Error(t, err, "rolled-back insert must not be visible outside the transaction")
}
