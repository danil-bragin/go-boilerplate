package ratelimit_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/web/ratelimit"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// newRedisAddr starts a Redis testcontainer and returns its "host:port" address.
// The container is cleaned up via t.Cleanup.
func newRedisAddr(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
	ctx := context.Background()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Terminate(context.Background()) })

	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)

	// Strip "redis://" scheme — rueidis wants "host:port".
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		addr = addr[len(prefix):]
	}
	return addr
}

// newRedisClient creates a rueidis client pointed at addr and registers cleanup.
func newRedisClient(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// TestRedis_BurstThenDeny verifies that burst tokens are granted and the next
// request is denied.
func TestRedis_BurstThenDeny(t *testing.T) {
	addr := newRedisAddr(t)
	client := newRedisClient(t, addr)
	limiter := ratelimit.NewRedis(client, 1, 3)
	ctx := context.Background()

	for i := range 3 {
		res, err := limiter.Allow(ctx, "burst-key")
		require.NoError(t, err)
		assert.True(t, res.Allowed, "allow %d must succeed (burst=3)", i+1)
		assert.EqualValues(t, 3, res.Limit, "Limit must report the bucket capacity")
		assert.EqualValues(t, 3-(i+1), res.Remaining, "Remaining must count down with each allow")
	}

	res, err := limiter.Allow(ctx, "burst-key")
	require.NoError(t, err)
	assert.False(t, res.Allowed, "4th allow must be denied after burst exhausted")
	assert.EqualValues(t, 0, res.Remaining)
	assert.Greater(t, res.RetryAfter, time.Duration(0), "denied result must carry the real refill wait")
	assert.LessOrEqual(t, res.RetryAfter, time.Second+100*time.Millisecond, "at 1 rps the wait is ≈1s")
}

// TestRedis_KeyIsolation verifies that two distinct keys have independent budgets.
func TestRedis_KeyIsolation(t *testing.T) {
	addr := newRedisAddr(t)
	client := newRedisClient(t, addr)
	limiter := ratelimit.NewRedis(client, 1, 2)
	ctx := context.Background()

	// Exhaust key A.
	_, _ = limiter.Allow(ctx, "iso-A")
	_, _ = limiter.Allow(ctx, "iso-A")
	resA, err := limiter.Allow(ctx, "iso-A")
	require.NoError(t, err)
	assert.False(t, resA.Allowed, "A must be denied after burst exhausted")

	// B must still be allowed.
	resB, err := limiter.Allow(ctx, "iso-B")
	require.NoError(t, err)
	assert.True(t, resB.Allowed, "B must be unaffected by A's exhaustion")
}

// TestRedis_RefillAfterSleep verifies that a 1 rps limiter refills one token
// after ~1 second.
func TestRedis_RefillAfterSleep(t *testing.T) {
	addr := newRedisAddr(t)
	client := newRedisClient(t, addr)
	// burst=1, rps=1 → exactly one token, refills 1/s.
	limiter := ratelimit.NewRedis(client, 1, 1)
	ctx := context.Background()

	res, err := limiter.Allow(ctx, "refill-key")
	require.NoError(t, err)
	require.True(t, res.Allowed, "first allow must succeed")

	res, err = limiter.Allow(ctx, "refill-key")
	require.NoError(t, err)
	require.False(t, res.Allowed, "second allow must be denied immediately")

	// Wait for one token to refill. 1.1s is generous to avoid flakiness.
	time.Sleep(1100 * time.Millisecond)

	res, err = limiter.Allow(ctx, "refill-key")
	require.NoError(t, err)
	assert.True(t, res.Allowed, "allow after ~1s must succeed (token refilled)")
}

// TestRedis_DistributedSharedBudget is the distributed-proof test.
//
// Two independent Redis clients (simulating two application replicas) share
// the same Redis instance. With burst=10, firing 10 Allow calls split across
// both clients should exhaust the budget, so the 11th call (on either) is denied.
func TestRedis_DistributedSharedBudget(t *testing.T) {
	addr := newRedisAddr(t)
	// Two separate clients — simulates two replicas.
	client1 := newRedisClient(t, addr)
	client2 := newRedisClient(t, addr)

	limiter1 := ratelimit.NewRedis(client1, 100, 10)
	limiter2 := ratelimit.NewRedis(client2, 100, 10)
	ctx := context.Background()

	const key = "distributed-key"

	// Fire 5 allows on each limiter (10 total, exactly at burst).
	for i := range 5 {
		res, err := limiter1.Allow(ctx, key)
		require.NoError(t, err)
		assert.True(t, res.Allowed, "limiter1 allow %d must succeed", i+1)

		res, err = limiter2.Allow(ctx, key)
		require.NoError(t, err)
		assert.True(t, res.Allowed, "limiter2 allow %d must succeed", i+1)
	}

	// 11th call on either limiter must be denied — shared budget via Redis.
	res1, err1 := limiter1.Allow(ctx, key)
	require.NoError(t, err1)
	res2, err2 := limiter2.Allow(ctx, key)
	require.NoError(t, err2)

	// At least one of the two 11th calls must be denied.
	assert.False(t, res1.Allowed && res2.Allowed,
		"at least one 11th allow must be denied across both replicas (shared budget)")
}

// TestRedis_FailOpen verifies that when Redis becomes unreachable mid-flight,
// Allow returns (true, nil) and the onError callback is invoked.
//
// Strategy: start a real container, create the client (which connects
// successfully), terminate the container, then call Allow. This avoids the
// rueidis dial-at-construction-time check that would reject a dead-address
// client immediately.
func TestRedis_FailOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
	ctx := context.Background()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		addr = addr[len(prefix):]
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Verify the connection works before killing the container.
	limiter := ratelimit.NewRedis(
		client, 1, 10,
		ratelimit.WithOnError(func(error) {}),
	)
	res, err := limiter.Allow(ctx, "pre-kill")
	require.NoError(t, err)
	require.True(t, res.Allowed, "must allow before container is killed")

	// Kill the container — subsequent calls must hit a dead connection.
	require.NoError(t, rc.Terminate(ctx))

	var errCount atomic.Int64
	limiter2 := ratelimit.NewRedis(
		client, 1, 1,
		ratelimit.WithOnError(func(error) { errCount.Add(1) }),
	)

	res, err = limiter2.Allow(ctx, "fail-open-key")
	assert.NoError(t, err, "fail-open must not propagate error to caller")
	assert.True(t, res.Allowed, "fail-open must allow the request")
	assert.Greater(t, errCount.Load(), int64(0), "onError must have been called")
}

// TestRedis_FailClosed verifies that when Redis becomes unreachable and
// WithFailClosed is set, Allow returns (false, non-nil error).
func TestRedis_FailClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redis container)")
	}
	ctx := context.Background()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	const prefix = "redis://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		addr = addr[len(prefix):]
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Warm up the connection.
	limiter := ratelimit.NewRedis(client, 1, 10)
	res, err := limiter.Allow(ctx, "pre-kill")
	require.NoError(t, err)
	require.True(t, res.Allowed, "must allow before container is killed")

	// Kill the container.
	require.NoError(t, rc.Terminate(ctx))

	var errCount atomic.Int64
	limiter2 := ratelimit.NewRedis(
		client, 1, 1,
		ratelimit.WithFailClosed(),
		ratelimit.WithOnError(func(error) { errCount.Add(1) }),
	)

	res, err = limiter2.Allow(ctx, "fail-closed-key")
	assert.Error(t, err, "fail-closed must propagate the error")
	assert.False(t, res.Allowed, "fail-closed must deny the request on error")
	assert.Greater(t, errCount.Load(), int64(0), "onError must have been called")
}
