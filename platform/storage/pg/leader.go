package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// leaderRetryInterval is the default cadence at which a non-leader retries
// lock acquisition and a leader health-checks its lock connection. Failover
// after a leader crash therefore takes a few seconds (lock-session death is
// detected on the next health tick; standbys re-try on the next retry tick).
const leaderRetryInterval = time.Second

// leaderReleaseTimeout bounds the detached graceful-unlock call that runs
// after ctx is already cancelled.
const leaderReleaseTimeout = 5 * time.Second

// leaderLockSQL acquires (non-blocking) the named leader lock on the CURRENT
// SESSION. hashtext maps the textual key to the int advisory-lock space. The
// key includes current_schema() so multiple services sharing one database
// (different schemas) elect independent leaders for the same worker name.
const leaderLockSQL = `select pg_try_advisory_lock(hashtext('leader:' || $1::text || ':' || current_schema()))`

// leaderUnlockSQL releases the named leader lock held by the current session.
const leaderUnlockSQL = `select pg_advisory_unlock(hashtext('leader:' || $1::text || ':' || current_schema()))`

// RunAsLeader runs fn on at most ONE instance at a time across all processes
// sharing the same database, elected via a session-scoped Postgres advisory
// lock keyed by name (schema-scoped, see leaderLockSQL).
//
// Mechanics:
//   - A dedicated connection is acquired from pool and holds
//     pg_try_advisory_lock(hashtext('leader:<name>:<schema>')). The instance
//     that wins the lock runs fn; all others stay hot-standby and re-try
//     every leaderRetryInterval.
//   - While fn runs, the lock connection is health-checked every
//     leaderRetryInterval (SELECT 1). A dead connection means the lock
//     session is gone server-side: fn's context is CANCELLED, the connection
//     is destroyed (not returned to the pool), and the instance re-joins the
//     acquisition loop — fn may therefore be invoked once per leadership
//     term and must return promptly when its context is cancelled.
//   - When ctx is cancelled, fn's context is cancelled, the lock is released
//     explicitly on a short detached context (graceful hand-off: a standby
//     can take over on its next retry tick), and ctx.Err() is returned.
//   - If fn returns on its own while leadership is still held, the lock is
//     released and fn's error (possibly nil) is returned — RunAsLeader does
//     not restart a worker that decided to stop.
//
// FENCING CAVEAT: an advisory lock is leader ELECTION, not fencing. A leader
// that loses its lock session only notices at the next health check, so
// there is a window of up to one leaderRetryInterval in which the old leader
// and a freshly elected one both run fn. Workers must therefore be safe
// under brief overlap (idempotent writes, compare-and-set guards, …).
func RunAsLeader(ctx context.Context, pool *pgxpool.Pool, name string, fn func(context.Context) error) error {
	return runAsLeader(ctx, pool, name, fn, leaderRetryInterval)
}

// runAsLeader is RunAsLeader with an injectable retry/health interval (tests).
func runAsLeader(ctx context.Context, pool *pgxpool.Pool, name string, fn func(context.Context) error, interval time.Duration) error {
	for {
		conn := tryAcquireLeaderLock(ctx, pool, name)
		if conn == nil {
			// Not the leader (or transient acquire failure): retry next tick.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
			continue
		}

		lost, err := leadTerm(ctx, conn, name, fn, interval)
		if !lost {
			return err
		}
		// Lock session lost: fn was cancelled and the dead connection
		// destroyed — re-join the acquisition loop with everyone else.
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// tryAcquireLeaderLock makes one non-blocking attempt to take the named
// leader lock on a dedicated pool connection. Returns the lock-holding
// connection, or nil when the lock is held elsewhere or any error occurred.
func tryAcquireLeaderLock(ctx context.Context, pool *pgxpool.Pool, name string) *pgxpool.Conn {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil
	}
	var acquired bool
	if err := conn.QueryRow(ctx, leaderLockSQL, name).Scan(&acquired); err != nil || !acquired {
		conn.Release()
		return nil
	}
	return conn
}

// leadTerm runs ONE leadership term: fn is started with a cancellable child
// context and the lock connection is health-checked every interval.
//
// Returns (false, fnErr) when the term ended normally (ctx cancelled or fn
// returned on its own) — the lock has been released. Returns (true, nil)
// when the lock session was lost: fn has been cancelled and awaited, and the
// dead connection destroyed; the caller re-contests the lock.
func leadTerm(ctx context.Context, conn *pgxpool.Conn, name string, fn func(context.Context) error, interval time.Duration) (lost bool, _ error) {
	fnCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	fnDone := make(chan error, 1)
	go func() { fnDone <- fn(fnCtx) }()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: stop fn, then release the lock explicitly on
			// a short detached context so a standby takes over immediately.
			cancelFn()
			<-fnDone
			releaseLeaderLock(ctx, conn, name)
			return false, ctx.Err()

		case err := <-fnDone:
			// fn returned on its own — stop being leader, propagate.
			releaseLeaderLock(ctx, conn, name)
			return false, err

		case <-ticker.C:
			// Health-check the dedicated lock connection. Any error ⇒ the
			// session (and the advisory lock with it) is gone server-side.
			if _, err := conn.Exec(ctx, `select 1`); err != nil {
				cancelFn()
				<-fnDone
				// Destroy the dead connection instead of returning it to the
				// pool — a parked lock-holding session would never release.
				_ = conn.Hijack().Close(ctx)
				return true, nil
			}
		}
	}
}

// releaseLeaderLock unlocks and returns the connection to the pool; if the
// unlock fails the session is destroyed so the server releases the lock.
// Runs on a detached context because ctx may already be cancelled.
func releaseLeaderLock(ctx context.Context, conn *pgxpool.Conn, name string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderReleaseTimeout)
	defer cancel()
	if _, err := conn.Exec(releaseCtx, leaderUnlockSQL, name); err == nil {
		conn.Release()
		return
	}
	_ = conn.Hijack().Close(releaseCtx)
}
