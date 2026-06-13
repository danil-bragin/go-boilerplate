package servicekit

import (
	"context"
	"errors"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
)

// AddOutboxPartitionMaintenance wires the opt-in outbox partition maintenance
// worker (ADR-0016). It is a NO-OP unless cfg.Mode == outbox.ModePartitioned,
// so the default "simple" mode (one DEFAULT partition, age-based DeletePublished
// cleanup) carries zero overhead and this can be called unconditionally.
//
// In partitioned mode it:
//   - eagerly pre-creates the current + lookahead partitions at wiring time so
//     the very first inserts route into a time-range partition instead of the
//     DEFAULT one (best-effort: a failure is logged, not fatal — rows simply
//     fall into DEFAULT until the first tick repairs it);
//   - registers a SINGLE-ACTIVE periodic worker (leader-elected, so partition
//     DDL is never issued by two instances at once) that re-ensures future
//     partitions and DETACH+DROPs expired ones. The worker NEVER drops a
//     partition still holding unpublished rows.
//
// Returns an error when the service was built with WithoutPG (the outbox lives
// in Postgres) or when called after Start.
func (s *Service) AddOutboxPartitionMaintenance(cfg outbox.PartitionConfig) error {
	if cfg.Mode != outbox.ModePartitioned {
		return nil // simple mode: nothing to wire
	}
	if s.pool == nil {
		return errors.New("servicekit: AddOutboxPartitionMaintenance requires postgres, but the service was built with WithoutPG()")
	}

	// One partition manager PER physical shard: the partitioned outbox table
	// lives in every database, so partition DDL (ensure future / drop expired)
	// must run on EVERY shard, not just shard 0. M=1 ⇒ one manager, identical
	// to today.
	physical := s.shards.Shards()
	managers := make([]*outbox.PartitionManager, len(physical))
	for i, pool := range physical {
		pIdx := i
		managers[i] = outbox.NewPartitionManager(pool, cfg, outbox.WithPartitionOnError(func(err error) {
			s.logger.Warn("outbox partition maintenance", "pshard", pIdx, "error", err)
		}))
	}

	// Eager pre-create on every shard so the first inserts route correctly.
	// Best-effort and bounded — never block service wiring on it.
	eagerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	now := time.Now().UTC()
	if err := s.shards.ForEachShard(eagerCtx, func(i int, _ *pg.Pool) error {
		return managers[i].EnsurePartitions(eagerCtx, now)
	}); err != nil {
		s.logger.Warn("outbox partition pre-create at startup failed (rows fall into DEFAULT until first maintenance tick)", "error", err)
	}
	cancel()

	// Maintenance cadence: frequent enough to pre-create well before the
	// partition window rolls over and to prune promptly, but decoupled from the
	// (much larger) partition width. Clamp to [1h, 24h], and never exceed the
	// interval itself so small (test) intervals still tick sensibly.
	cadence := cfg.Interval / 4
	if cadence > 24*time.Hour {
		cadence = 24 * time.Hour
	}
	if cadence < time.Hour {
		cadence = time.Hour
	}
	if cfg.Interval > 0 && cadence > cfg.Interval {
		cadence = cfg.Interval
	}

	// Single leader-elected worker (leader lock on shard 0) that fans Maintain
	// across every physical shard's manager, so partition DDL is never issued
	// by two instances at once yet runs on every database.
	return s.AddPeriodicWorker("outbox-partition-maintenance", cadence, cadence/10, true, func(ctx context.Context) error {
		return s.shards.ForEachShard(ctx, func(i int, _ *pg.Pool) error {
			return managers[i].Maintain(ctx)
		})
	})
}
