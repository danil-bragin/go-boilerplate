package outbox_test

import (
	"context"
	"embed"
	"testing"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//go:embed migrations/*.sql
var migrations embed.FS

func newPoolWithSchema(t *testing.T) *pg.Pool {
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

func TestEnqueue_WritesRowWithinTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID:            id,
			AggregateType: "order",
			AggregateID:   "42",
			EventType:     "OrderCreated",
			Payload:       []byte(`{"id":"42"}`),
		})
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, pool.Reader().QueryRow(
		ctx,
		`select count(*) from outbox where id=$1 and published_at is null`, id,
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEnqueue_RolledBackWithFailedTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_ = repo.Enqueue(ctx, outbox.Message{
			ID: id, AggregateType: "order", AggregateID: "1",
			EventType: "X", Payload: []byte(`{}`),
		})
		return context.Canceled // force rollback
	})

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id=$1`, id).Scan(&count))
	require.Equal(t, 0, count, "enqueue must roll back with the business tx")
}
