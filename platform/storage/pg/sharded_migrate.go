package pg

import (
	"context"
	"io/fs"
)

// MigrateSharded runs Migrate against every physical shard concurrently (via
// ForEachShard). Each shard migrates independently and atomically — the
// per-connection advisory lock in Migrate holds per shard. M=1 ⇒ exactly one
// Migrate, identical to the single-pool path. Migrations MUST be expand-contract:
// they run per shard and are NOT atomic across the fleet (see ADR-0019).
func MigrateSharded(ctx context.Context, sp *ShardedPool, fsys fs.FS, dir string) error {
	return sp.ForEachShard(ctx, func(idx int, _ *Pool) error {
		return Migrate(ctx, sp.migrateURLs[idx], fsys, dir)
	})
}
