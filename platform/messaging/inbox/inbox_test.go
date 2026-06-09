package inbox_test

import (
	"context"
	"embed"
	"errors"
	"testing"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/pg"
	"go-boilerplate/platform/pg/pgtest"

	"github.com/stretchr/testify/require"
)

//go:embed migrations/*.sql
var migrations embed.FS

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

func TestProcessOnce_DedupesByConsumer(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	var counter int
	fn := func(_ context.Context) error {
		counter++
		return nil
	}

	// First call: should process and run fn.
	processed, err := inbox.ProcessOnce(ctx, pool, "c1", "m1", fn)
	require.NoError(t, err)
	require.True(t, processed, "first call should be processed")
	require.Equal(t, 1, counter, "fn should have run once")

	// Second call with same (consumer, messageID): should be a no-op.
	processed, err = inbox.ProcessOnce(ctx, pool, "c1", "m1", fn)
	require.NoError(t, err)
	require.False(t, processed, "second call should not be processed (duplicate)")
	require.Equal(t, 1, counter, "fn should NOT have run again")
}

func TestProcessOnce_FnErrorRollsBackMarker(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	errBoom := errors.New("boom")

	// First call: fn returns error — tx rolls back, inbox row is NOT persisted.
	_, err := inbox.ProcessOnce(ctx, pool, "c1", "m2", func(_ context.Context) error {
		return errBoom
	})
	require.ErrorIs(t, err, errBoom, "ProcessOnce should propagate fn's error")

	// Second call: marker was rolled back, so this should succeed now.
	processed, err := inbox.ProcessOnce(ctx, pool, "c1", "m2", func(_ context.Context) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, processed, "second call should succeed since marker was rolled back")
}

func TestProcessOnce_DifferentConsumersEachProcessOnce(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	var c1Count, c2Count int

	processed1, err := inbox.ProcessOnce(ctx, pool, "c1", "m3", func(_ context.Context) error {
		c1Count++
		return nil
	})
	require.NoError(t, err)
	require.True(t, processed1, "c1 should process m3")

	processed2, err := inbox.ProcessOnce(ctx, pool, "c2", "m3", func(_ context.Context) error {
		c2Count++
		return nil
	})
	require.NoError(t, err)
	require.True(t, processed2, "c2 should also process m3 (composite PK allows it)")

	require.Equal(t, 1, c1Count, "c1 fn ran once")
	require.Equal(t, 1, c2Count, "c2 fn ran once")
}
