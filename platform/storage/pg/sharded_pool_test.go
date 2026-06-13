package pg_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// newShardedTestPool spins n fresh Postgres containers and returns a ShardedPool
// over them. Each pgtest.NewDSN call is a self-cleaning container.
func newShardedTestPool(t *testing.T, n int) *pg.ShardedPool {
	t.Helper()
	ctx := context.Background()
	dsns := make([]config.Secret, n)
	for i := range n {
		dsns[i] = config.Secret(pgtest.NewDSN(t))
	}
	sp, err := pg.NewSharded(ctx, pg.ShardedConfig{DSNs: dsns})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sp.Close(context.Background()) })
	return sp
}

func TestShardedPool_ResolveStableAndInRange(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 2)
	require.Equal(t, 2, sp.Len())

	p := sp.Resolve("order-1")
	require.NotNil(t, p)
	require.Same(t, p, sp.Resolve("order-1"), "same key ⇒ same pool")
	require.Contains(t, sp.Shards(), p)
}

func TestShardedPool_SingleShardIsOnePool(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 1)
	require.Equal(t, 1, sp.Len())
	require.Same(t, sp.Shards()[0], sp.Resolve("anything"))
}
