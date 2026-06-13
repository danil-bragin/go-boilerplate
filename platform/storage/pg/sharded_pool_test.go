package pg_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/fstest"

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

func TestShardedPool_ForEachShard_RunsAllConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 3)
	var mu sync.Mutex
	seen := map[int]bool{}
	err := sp.ForEachShard(context.Background(), func(idx int, p *pg.Pool) error {
		require.NotNil(t, p)
		mu.Lock()
		seen[idx] = true
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[int]bool{0: true, 1: true, 2: true}, seen)
}

func TestShardedPool_ForEachShard_JoinsErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 3)
	err := sp.ForEachShard(context.Background(), func(idx int, _ *pg.Pool) error {
		if idx == 1 {
			return errors.New("shard 1 boom")
		}
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard 1")
}

func TestShardedPool_HealthCheck_AllUp(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 2)
	require.NoError(t, sp.HealthCheck(context.Background()))
}

func TestShardedPool_RunInTx_RoutesByKeyAndIsolates(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	sp := newShardedTestPool(t, 2)
	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		_, err := p.Writer().Exec(ctx, `create table k (v text primary key)`)
		return err
	}))

	const key = "order-77"
	kctx := pg.WithShardKey(ctx, key)
	require.NoError(t, sp.RunInTx(kctx, func(ctx context.Context) error {
		db, err := sp.FromContext(ctx)
		if err != nil {
			return err
		}
		_, err = db.Exec(ctx, `insert into k (v) values ($1)`, "hello")
		return err
	}))

	owner := sp.Resolve(key)
	var n int
	require.NoError(t, owner.Reader().QueryRow(ctx, `select count(*) from k`).Scan(&n))
	require.Equal(t, 1, n, "row must land on the resolved shard")
	for _, p := range sp.Shards() {
		if p == owner {
			continue
		}
		var m int
		require.NoError(t, p.Reader().QueryRow(ctx, `select count(*) from k`).Scan(&m))
		require.Equal(t, 0, m, "no other shard may hold the row")
	}
}

func TestShardedPool_RunInTx_RollsBack(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	sp := newShardedTestPool(t, 2)
	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		_, err := p.Writer().Exec(ctx, `create table k (v text primary key)`)
		return err
	}))
	kctx := pg.WithShardKey(ctx, "order-1")
	wantErr := errors.New("boom")
	err := sp.RunInTx(kctx, func(ctx context.Context) error {
		db, ferr := sp.FromContext(ctx)
		require.NoError(t, ferr)
		_, _ = db.Exec(ctx, `insert into k (v) values ('x')`)
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	var n int
	require.NoError(t, sp.Resolve("order-1").Reader().QueryRow(ctx, `select count(*) from k`).Scan(&n))
	require.Equal(t, 0, n, "rollback must undo the insert")
}

func TestShardedPool_NoShardKey_FailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	sp := newShardedTestPool(t, 2)
	err := sp.RunInTx(context.Background(), func(context.Context) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard key")
}

func TestMigrateSharded_AppliesToEveryShard(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	sp := newShardedTestPool(t, 2)

	fsys := fstest.MapFS{
		"sql/00001_widgets.sql": &fstest.MapFile{Data: []byte(
			"-- +goose Up\ncreate table widgets (id bigserial primary key);\n-- +goose Down\ndrop table widgets;\n",
		)},
	}
	require.NoError(t, pg.MigrateSharded(ctx, sp, fsys, "sql"))

	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		var ok bool
		err := p.Reader().QueryRow(ctx,
			`select exists (select 1 from information_schema.tables where table_name='widgets')`).Scan(&ok)
		if err != nil {
			return err
		}
		require.True(t, ok)
		return nil
	}))
}
