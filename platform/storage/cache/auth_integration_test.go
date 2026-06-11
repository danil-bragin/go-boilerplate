package cache_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/cache"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// newRedisWithPassword starts a Redis testcontainer protected by requirepass
// and returns its "host:port" address. Cleaned up via t.Cleanup.
func newRedisWithPassword(t *testing.T, password string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
	ctx := context.Background()

	rc, err := tcredis.Run(
		ctx,
		"redis:7-alpine",
		testcontainers.WithCmd("redis-server", "--requirepass", password),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rc.Terminate(context.Background())
	})

	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	return stripRedisScheme(addr)
}

// TestCache_RedisAuth_CorrectPassword proves the cache authenticates to a
// password-protected Redis when Config.Password is set: a Set/Get round-trips
// through L2.
func TestCache_RedisAuth_CorrectPassword(t *testing.T) {
	const password = "s3cr3t-pass"
	addr := newRedisWithPassword(t, password)

	cfg := cache.Config{
		RedisAddrs: []string{addr},
		Password:   config.Secret(password),
		L1Capacity: 100,
		DefaultTTL: time.Minute,
	}
	c, err := cache.New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	ctx := context.Background()
	require.NoError(t, c.HealthCheck(ctx), "authenticated PING must succeed")

	c.Set(ctx, "k", []byte("v"), time.Minute)
	// Drop L1 by closing/reopening would be heavy; instead verify via a fresh
	// authenticated rueidis client reads the L2 value directly.
	rc, err := rueidis.NewClient(cache.BuildRueidisOption(cfg))
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	got, err := rc.Do(ctx, rc.B().Get().Key("k").Build()).ToString()
	require.NoError(t, err)
	assert.Equal(t, "v", got)
}

// TestCache_RedisAuth_WrongPassword proves authentication actually gates
// access: rueidis connects eagerly in cache.New, so a wrong/empty password
// against a requirepass Redis fails closed at construction (WRONGPASS/NOAUTH)
// rather than silently degrading.
func TestCache_RedisAuth_WrongPassword(t *testing.T) {
	addr := newRedisWithPassword(t, "the-real-password")

	// Wrong password.
	_, err := cache.New(cache.Config{
		RedisAddrs: []string{addr},
		Password:   config.Secret("not-the-password"),
		L1Capacity: 100,
		DefaultTTL: time.Minute,
	})
	require.Error(t, err, "wrong password must be rejected (WRONGPASS)")

	// Empty password against a requirepass server.
	_, err = cache.New(cache.Config{
		RedisAddrs: []string{addr},
		L1Capacity: 100,
		DefaultTTL: time.Minute,
	})
	require.Error(t, err, "missing password must be rejected (NOAUTH)")
}
