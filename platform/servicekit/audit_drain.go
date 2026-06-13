package servicekit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
)

// AddAuditDrain wires the durable-audit drain: a single-active (leader-elected)
// periodic worker PER physical shard that applies staged audit intents
// (audit_pending) to the hash chain via DrainPending. Single-active because a
// hash chain must be applied strictly in order per chain (DrainPending reads
// audit_pending ordered by (chain_id, id) and is documented to be called
// single-active per shard). The per-shard audit.PgStore is built with the SAME
// chain options (key/shards) as the command-side store — pass them via opts.
//
// Each physical shard runs its own leader-elected drain loop on its own pool:
// pg.RunAsLeader holds a session-scoped advisory lock keyed by the worker name
// (already schema-namespaced via current_schema(); see leader.go) and only the
// elected instance runs the loop. The lock NAME uses a distinct "audit_drain"
// prefix so it never collides with the relay's "outbox_relay" lock, and at M>1
// gets a ":pshard:<i>" suffix so shard 0's leader and shard 1's leader do not
// contend for the same lock. At M=1 (PG_SHARDS unset) the name is the bare,
// stable "audit_drain".
//
// pg.RunAsLeader retries lock acquisition internally (its loop re-contests the
// lock every leaderRetryInterval until ctx is cancelled) and keeps the loop
// running across ticks while leadership is held, so this method does NOT
// re-acquire per tick. On clean shutdown (ctx cancel) RunAsLeader cancels the
// loop, releases the lock, and returns ctx.Err().
//
// Must be called before Start; interval<=0 disables it. Returns an error when
// the service was built with WithoutPG (audit_pending lives in Postgres).
func (s *Service) AddAuditDrain(interval time.Duration, batchSize int, opts ...audit.Option) error {
	if interval <= 0 {
		return nil
	}
	if s.started {
		return errors.New("servicekit: AddAuditDrain called after Start — the worker would never run")
	}
	if s.shards == nil {
		return errors.New("servicekit: AddAuditDrain requires postgres, but the service was built with WithoutPG()")
	}

	physical := s.shards.Shards()
	multiPhysical := len(physical) > 1
	for pIdx, pool := range physical {
		// Distinct prefix from the relay's "outbox_relay" lock; ":pshard:<i>"
		// suffix only at M>1 so the M=1 name stays bare and stable.
		name := "audit_drain"
		if multiPhysical {
			name = fmt.Sprintf("%s:pshard:%d", name, pIdx)
		}
		idx := pIdx
		drainPool := pool
		drainStore := audit.NewPgStore(pool, opts...)
		leaderName := name
		s.goroutines = append(s.goroutines, func(ctx context.Context) {
			// RunAsLeader retries acquisition internally until ctx is done, so
			// the worker keeps trying to become leader for its whole lifetime.
			err := pg.RunAsLeader(ctx, drainPool.Writer(), leaderName, func(ctx context.Context) error {
				t := time.NewTicker(interval)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-t.C:
						// Drain to empty each tick; then report backlog.
						for {
							applied, derr := drainStore.DrainPending(ctx, batchSize)
							if derr != nil {
								// Leave rows pending; retry next tick — no loss.
								s.logger.Error("audit drain error", "pshard", idx, "error", derr)
								break
							}
							if applied == 0 {
								break
							}
						}
						if n, berr := drainStore.PendingBacklog(ctx); berr == nil {
							drainStore.RecordPendingBacklog(ctx, n)
						}
					}
				}
			})
			if err != nil && ctx.Err() == nil {
				s.logger.Error("audit drain stopped unexpectedly", "pshard", idx, "error", err)
			}
		})
	}
	return nil
}
