package pg

// Integration tests for RunAsLeader: single-active election across two
// instances, fn-context cancellation on lock loss, failover, and graceful
// hand-off on context cancellation. White-box (package pg) so the tests can
// use runAsLeader with a short retry interval.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/platform/testkit/goleakopts"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// leaderHarness is one RunAsLeader instance under test: fn marks running
// while it holds leadership and exits when its ctx is cancelled.
type leaderHarness struct {
	running   atomic.Bool
	terms     atomic.Int64 // number of leadership terms fn was started for
	cancelled atomic.Int64 // number of times fn observed ctx cancellation
	cancel    context.CancelFunc
	done      chan struct{}
}

// startLeader launches runAsLeader(name) on pool with a 50ms retry interval.
func startLeader(t *testing.T, pool *Pool, name string) *leaderHarness {
	t.Helper()
	h := &leaderHarness{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		defer close(h.done)
		_ = runAsLeader(ctx, pool.Writer(), name, func(fnCtx context.Context) error {
			h.terms.Add(1)
			h.running.Store(true)
			<-fnCtx.Done()
			h.running.Store(false)
			h.cancelled.Add(1)
			return fnCtx.Err()
		}, 50*time.Millisecond)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
			t.Error("runAsLeader goroutine did not exit after cancel")
		}
	})
	return h
}

func twoLeaderPools(t *testing.T) (*Pool, *Pool) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()
	pool1, err := New(ctx, Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool1.Close(ctx) })
	pool2, err := New(ctx, Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool2.Close(ctx) })
	return pool1, pool2
}

// exactlyOneRunning reports whether exactly one of the two harnesses is
// currently the leader.
func exactlyOneRunning(a, b *leaderHarness) bool {
	return a.running.Load() != b.running.Load()
}

// TestRunAsLeader_SingleActive_FnCancelledOnLockLoss_Failover verifies:
//  1. With two instances competing for the same name, exactly ONE runs fn.
//  2. When the leader's advisory-lock session is killed server-side, the
//     leader detects it and CANCELS fn's context.
//  3. Leadership is re-contested: eventually exactly one instance runs again.
func TestRunAsLeader_SingleActive_FnCancelledOnLockLoss_Failover(t *testing.T) {
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })
	pool1, pool2 := twoLeaderPools(t)
	ctx := context.Background()

	h1 := startLeader(t, pool1, "worker-a")
	h2 := startLeader(t, pool2, "worker-a")

	require.Eventually(t, func() bool { return exactlyOneRunning(h1, h2) },
		10*time.Second, 10*time.Millisecond, "exactly one instance must become leader")
	// Give the standby a few intervals to (incorrectly) also start.
	time.Sleep(250 * time.Millisecond)
	require.True(t, exactlyOneRunning(h1, h2),
		"standby must stay idle: running1=%v running2=%v", h1.running.Load(), h2.running.Load())

	// Kill the advisory-lock session of the current leader (crash simulation).
	_, err := pool1.Writer().Exec(ctx, `
		select pg_terminate_backend(l.pid)
		from pg_locks l
		where l.locktype = 'advisory' and l.granted
		  and l.pid <> pg_backend_pid()`)
	require.NoError(t, err)

	// The deposed leader must observe lock loss and cancel fn's context.
	require.Eventually(t, func() bool { return h1.cancelled.Load()+h2.cancelled.Load() >= 1 },
		10*time.Second, 10*time.Millisecond, "fn ctx must be cancelled on lock loss")

	// Leadership is re-contested: exactly one runs again.
	require.Eventually(t, func() bool { return exactlyOneRunning(h1, h2) },
		10*time.Second, 10*time.Millisecond, "a new leader must be elected after lock loss")
}

// TestRunAsLeader_GracefulHandoffOnCancel verifies that cancelling the
// leader's context releases the advisory lock explicitly so the standby
// takes over promptly (no need to wait for a server-side session timeout).
func TestRunAsLeader_GracefulHandoffOnCancel(t *testing.T) {
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })
	pool1, pool2 := twoLeaderPools(t)

	h1 := startLeader(t, pool1, "worker-b")
	require.Eventually(t, func() bool { return h1.running.Load() },
		10*time.Second, 10*time.Millisecond, "first instance must become leader")

	h2 := startLeader(t, pool2, "worker-b")
	time.Sleep(150 * time.Millisecond)
	require.False(t, h2.running.Load(), "second instance must stay standby while the lock is held")

	// Graceful shutdown of the leader → standby must take over.
	h1.cancel()
	<-h1.done
	require.Eventually(t, func() bool { return h2.running.Load() },
		10*time.Second, 10*time.Millisecond, "standby must take over after graceful release")
}

// TestRunAsLeader_DistinctNamesRunConcurrently verifies the lock is scoped
// per worker name: two different names elect independent leaders.
func TestRunAsLeader_DistinctNamesRunConcurrently(t *testing.T) {
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })
	pool1, pool2 := twoLeaderPools(t)

	h1 := startLeader(t, pool1, "worker-c")
	h2 := startLeader(t, pool2, "worker-d")

	require.Eventually(t, func() bool { return h1.running.Load() && h2.running.Load() },
		10*time.Second, 10*time.Millisecond, "distinct names must not contend for the same lock")
}

// TestRunAsLeader_FnErrorStopsWorker verifies that when fn returns on its own
// (without ctx cancellation) RunAsLeader releases the lock and returns fn's
// error instead of re-running it.
func TestRunAsLeader_FnErrorStopsWorker(t *testing.T) {
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })
	pool1, pool2 := twoLeaderPools(t)
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runAsLeader(ctx, pool1.Writer(), "worker-e", func(context.Context) error {
			return context.DeadlineExceeded // any sentinel
		}, 50*time.Millisecond)
	}()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.DeadlineExceeded, "fn's own error must be returned")
	case <-time.After(10 * time.Second):
		t.Fatal("runAsLeader did not return after fn errored")
	}

	// The lock must have been released: a second instance can acquire it.
	h2 := startLeader(t, pool2, "worker-e")
	require.Eventually(t, func() bool { return h2.running.Load() },
		10*time.Second, 10*time.Millisecond, "lock must be free after fn returned")
}
