package servicekit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/platform/testkit/goleakopts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestAddPeriodicWorker_RunsAndStops: a periodic worker fires repeatedly at
// its interval, fn errors are logged not fatal (the loop keeps ticking), and
// Stop cancels and waits for the goroutine. Short mode (no containers).
func TestAddPeriodicWorker_RunsAndStops(t *testing.T) {
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })

	cfg := optsConfig()
	svc, err := servicekit.New(context.Background(), cfg, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	require.NoError(t, err)

	var ticks atomic.Int64
	require.NoError(t, svc.AddPeriodicWorker("test-periodic", 10*time.Millisecond, 0, false,
		func(context.Context) error {
			ticks.Add(1)
			return errors.New("boom") // must be logged, not stop the loop
		}))

	require.NoError(t, svc.Start())
	require.Eventually(t, func() bool { return ticks.Load() >= 3 },
		5*time.Second, 5*time.Millisecond, "worker must keep firing despite fn errors")

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Stop(stopCtx))
	final := ticks.Load()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, final, ticks.Load(), "worker must not fire after Stop")
}

// TestAddPeriodicWorker_InvalidArgs: a non-positive interval is rejected, and
// singleActive requires Postgres (WithoutPG → error). Short mode.
func TestAddPeriodicWorker_InvalidArgs(t *testing.T) {
	cfg := optsConfig()
	svc, err := servicekit.New(context.Background(), cfg, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	require.NoError(t, err)

	err = svc.AddPeriodicWorker("bad-interval", 0, 0, false, func(context.Context) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interval")

	err = svc.AddPeriodicWorker("needs-pg", time.Second, 0, true, func(context.Context) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithoutPG")
}

// TestAddPeriodicWorker_SingleActiveAcrossTwoServices: two Service instances
// sharing one Postgres database register the same singleActive worker; the
// advisory-lock leader election must let EXACTLY ONE of them tick. When the
// active instance stops gracefully, the standby takes over.
func TestAddPeriodicWorker_SingleActiveAcrossTwoServices(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	goleakOpts := goleakopts.Default(goleak.IgnoreCurrent())
	t.Cleanup(func() { goleak.VerifyNone(t, goleakOpts...) })

	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()

	newSvc := func() *servicekit.Service {
		cfg := servicekit.Config{AdminAddr: "127.0.0.1:0"}
		cfg.PG.DSN = config.Secret(dsn)
		cfg.Telemetry.Enabled = false
		cfg.Log.Level = "error"
		cfg.DrainGrace = 0
		cfg.InboxCleanupInterval = 0 // no inbox table in this harness test
		svc, err := servicekit.New(ctx, cfg, nil, "", servicekit.WithoutKafka())
		require.NoError(t, err)
		return svc
	}

	svc1, svc2 := newSvc(), newSvc()
	var ticks1, ticks2 atomic.Int64
	require.NoError(t, svc1.AddPeriodicWorker("shared-worker", 50*time.Millisecond, 0, true,
		func(context.Context) error { ticks1.Add(1); return nil }))
	require.NoError(t, svc2.AddPeriodicWorker("shared-worker", 50*time.Millisecond, 0, true,
		func(context.Context) error { ticks2.Add(1); return nil }))

	// stopOnce-protected Stop: the test stops the leader explicitly and the
	// cleanup stops whatever is still running (Closer is not re-runnable).
	stopSvc := func(svc *servicekit.Service) func() error {
		var once sync.Once
		var err error
		return func() error {
			once.Do(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err = svc.Stop(stopCtx)
			})
			return err
		}
	}
	stop1, stop2 := stopSvc(svc1), stopSvc(svc2)

	require.NoError(t, svc1.Start())
	t.Cleanup(func() { _ = stop1() })
	require.NoError(t, svc2.Start())
	t.Cleanup(func() { _ = stop2() })

	// Exactly one instance must tick.
	require.Eventually(t, func() bool { return ticks1.Load()+ticks2.Load() >= 3 },
		10*time.Second, 10*time.Millisecond, "the leader must start ticking")
	time.Sleep(300 * time.Millisecond)
	t1, t2 := ticks1.Load(), ticks2.Load()
	require.True(t, (t1 == 0) != (t2 == 0),
		"exactly one instance must run the singleActive worker: ticks1=%d ticks2=%d", t1, t2)

	// Stop the active instance → the standby must take over (graceful lock
	// release; failover within ~one leader retry interval).
	standbyTicks := &ticks2
	if t2 > 0 {
		standbyTicks = &ticks1
		require.NoError(t, stop2())
	} else {
		require.NoError(t, stop1())
	}
	require.Eventually(t, func() bool { return standbyTicks.Load() > 0 },
		15*time.Second, 20*time.Millisecond, "the standby must take over after the leader stopped")
}
