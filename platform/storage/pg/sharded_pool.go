package pg

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go-boilerplate/platform/config"
)

// ShardedConfig configures a ShardedPool. DSNs lists the writer DSN of each
// physical shard (1..M). ReaderDSNs, when non-empty, must have the same length
// and supplies a reader DSN per shard (a replica); an empty entry inherits the
// shard's writer. PerShard carries the tuning (pool sizes, timeouts, query
// metrics) applied to every shard; its DSN/ReaderDSN fields are ignored (the
// per-shard DSNs above win). NOTE: PerShard.MigrateURL, if set, is applied to
// ALL shards — only correct for M=1; for M>1 leave it empty so each shard's own
// DSN is the migrate target.
type ShardedConfig struct {
	DSNs       []config.Secret
	ReaderDSNs []config.Secret
	PerShard   Config
}

// ShardedPool routes each operation to one physical shard by a stable hash of an
// aggregate key carried in the context (see RunInTx/FromContext in
// sharded_tx.go). It is opt-in and independent of Pool; a ShardedPool over a
// single DSN behaves identically to that Pool. See ADR-0019.
type ShardedPool struct {
	shards      []*Pool
	router      *Router
	migrateURLs []string // per-shard DSN used by MigrateSharded
}

// NewSharded builds one Pool per DSN (reusing New with PerShard tuning) and a
// Router over len(DSNs). It closes any already-built pools and returns an error
// if a shard fails to connect.
func NewSharded(ctx context.Context, cfg ShardedConfig) (*ShardedPool, error) {
	if len(cfg.DSNs) == 0 {
		return nil, errors.New("pg: NewSharded requires at least one DSN")
	}
	if len(cfg.ReaderDSNs) != 0 && len(cfg.ReaderDSNs) != len(cfg.DSNs) {
		return nil, fmt.Errorf("pg: ReaderDSNs length %d != DSNs length %d", len(cfg.ReaderDSNs), len(cfg.DSNs))
	}
	if len(cfg.DSNs) > 1 && cfg.PerShard.MigrateURL != "" {
		// A single MigrateURL would point every shard's migrations at one
		// database — silent data corruption. Per-shard migrate targets come from
		// each shard's own DSN; MigrateURL is only valid for M=1 (e.g. when the
		// single DSN is behind PgBouncer and migrations need a direct session).
		return nil, errors.New("pg: ShardedConfig.PerShard.MigrateURL must be empty when sharding (M>1) — each shard migrates via its own DSN")
	}
	sp := &ShardedPool{
		shards:      make([]*Pool, 0, len(cfg.DSNs)),
		router:      NewRouter(len(cfg.DSNs)),
		migrateURLs: make([]string, 0, len(cfg.DSNs)),
	}
	for i, dsn := range cfg.DSNs {
		shardCfg := cfg.PerShard
		shardCfg.DSN = dsn
		if len(cfg.ReaderDSNs) != 0 {
			shardCfg.ReaderDSN = cfg.ReaderDSNs[i]
		} else {
			shardCfg.ReaderDSN = ""
		}
		p, err := New(ctx, shardCfg)
		if err != nil {
			_ = sp.Close(context.Background())
			return nil, fmt.Errorf("pg: build shard %d: %w", i, err)
		}
		sp.shards = append(sp.shards, p)
		migrateURL := dsn.Reveal()
		if cfg.PerShard.MigrateURL != "" {
			migrateURL = cfg.PerShard.MigrateURL.Reveal()
		}
		sp.migrateURLs = append(sp.migrateURLs, migrateURL)
	}
	return sp, nil
}

// Len is the number of physical shards.
func (sp *ShardedPool) Len() int { return len(sp.shards) }

// Shards returns the physical shard pools (index == physical shard id).
func (sp *ShardedPool) Shards() []*Pool { return sp.shards }

// Resolve returns the shard pool that owns key.
func (sp *ShardedPool) Resolve(key string) *Pool { return sp.shards[sp.router.Physical(key)] }

// Close closes every shard, joining any errors.
func (sp *ShardedPool) Close(ctx context.Context) error {
	var errs []error
	for i, p := range sp.shards {
		if p == nil {
			continue
		}
		if err := p.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pg: close shard %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// ForEachShard runs fn against every physical shard CONCURRENTLY and returns a
// joined error naming each failing shard. It is the fan-out primitive for
// keyless operations (global LIST, audit verify) and for sharded migrations. A
// partial failure is always surfaced — never a silent partial result.
func (sp *ShardedPool) ForEachShard(ctx context.Context, fn func(idx int, p *Pool) error) error {
	if err := ctx.Err(); err != nil {
		return err // do not fan out on an already-cancelled context
	}
	errs := make([]error, len(sp.shards))
	var wg sync.WaitGroup
	wg.Add(len(sp.shards))
	for i, p := range sp.shards {
		go func(i int, p *Pool) {
			defer wg.Done()
			if err := fn(i, p); err != nil {
				errs[i] = fmt.Errorf("pg: shard %d: %w", i, err)
			}
		}(i, p)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// HealthCheck reports healthy only if EVERY shard is healthy (readiness =
// all-shards-up). A single shard down fails the whole check, because that
// shard's aggregates cannot be written; see ADR-0019 known-limitations for the
// per-aggregate-availability alternative.
func (sp *ShardedPool) HealthCheck(ctx context.Context) error {
	return sp.ForEachShard(ctx, func(_ int, p *Pool) error { return p.HealthCheck(ctx) })
}
