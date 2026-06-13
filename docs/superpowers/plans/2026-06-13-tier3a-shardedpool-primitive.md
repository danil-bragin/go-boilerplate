# Tier-3 sub-project A — `pg.ShardedPool` primitive — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in `pg.ShardedPool` to `platform/storage/pg` that routes each DB operation to one physical Postgres shard by a stable, cross-process-deterministic hash of an aggregate key carried in the context — the headroom/linearity lever for >40k/s write paths, with `M=1` byte-identical to today.

**Architecture:** A new `ShardedPool` type holds `[]*pg.Pool` (one per shard DSN, reusing today's tuned writer/reader pool) plus a `Router` (FNV-1a 64-bit → 256 fixed logical shards → static physical assignment). The transaction seam mirrors the existing `RunInTx`/`FromContext` but resolves the pool from a context-carried shard key and delegates to the existing single-pool functions — so the ambient-tx machinery, reader split, and query tracer are all reused unchanged. `pg.Pool` and its functions are NOT modified.

**Tech Stack:** Go 1.26, pgx v5 / pgxpool, stdlib `hash/fnv`, testcontainers (Postgres), goose (existing `pg.Migrate`), testify.

**Spec:** `docs/superpowers/specs/2026-06-13-tier3-shardedpool-primitive-design.md`.

**Hard rules (every task preserves):**
1. No cross-shard transaction; a key maps to exactly one shard for life.
2. `pg.Pool`/`RunInTx`/`FromContext`/`Migrate` are NOT changed.
3. `M=1` ⇒ behaves byte-identically to a single `pg.Pool`.
4. Routing is deterministic across processes (pinned FNV-1a 64-bit, never `maphash`).
5. A keyed op with no shard key in context fails closed (error), never silently routes to shard 0.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `platform/storage/pg/sharded_router.go` | `Router`: pinned hash → logical → physical | Create |
| `platform/storage/pg/sharded_router_test.go` | determinism table, M=1, distribution | Create |
| `platform/storage/pg/sharded_pool.go` | `ShardedConfig`, `NewSharded`, `ShardedPool`, `Resolve`, `Shards`, `Len`, `ForEachShard`, `HealthCheck`, `Close` | Create |
| `platform/storage/pg/sharded_tx.go` | `WithShardKey`, `shardKeyFrom`, `(*ShardedPool) RunInTx/FromContext/FromContextRead` | Create |
| `platform/storage/pg/sharded_migrate.go` | `MigrateSharded` | Create |
| `platform/storage/pg/sharded_pool_test.go` | integration: routing, M=1 identity, ForEachShard, tx, migrate (2-shard testcontainers) | Create |
| `docs/adr/0019-postgres-sharding.md` | ADR (static M honesty + known limitations) | Create |

NB: all sharded files are in package `pg`, so they may use the unexported `txCtxKey{}` from `tx.go` — but the seam delegates to the exported `RunInTx`/`FromContext`, so it does not need to. Tests live in package `pg_test` (external) and use only the public API.

Confirm the ADR directory: run `ls docs/adr/ | tail -3` to confirm `0018-*` exists and `0019` is the next number; if ADRs live elsewhere, place 0019 beside the others.

---

## Task 1: `Router` — pinned hash, logical→physical

**Files:** Create `platform/storage/pg/sharded_router.go`, `platform/storage/pg/sharded_router_test.go`.

- [ ] **Step 1: Write the failing test**

```go
package pg_test

import (
	"testing"

	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

func TestRouter_DeterministicAcrossProcesses(t *testing.T) {
	// The cross-process guard: these key→physical mappings are pinned. If the
	// hash or logical-shard count ever changes, this table breaks — which is the
	// point (a silent change would split an aggregate across shards in different
	// services). Values are filled in from the first green run and then FROZEN.
	r := pg.NewRouter(4)
	got := map[string]int{}
	for _, k := range []string{"order-1", "order-2", "order-3", "order-abc", "order-xyz"} {
		got[k] = r.Physical(k)
	}
	// Freeze after first run (replace with the observed values, then assert):
	require.Equal(t, got["order-1"], r.Physical("order-1"), "stable within process")
	// Same Router, same key, always same shard:
	for k := range got {
		require.Equal(t, got[k], r.Physical(k))
	}
	// All within range.
	for _, v := range got {
		require.GreaterOrEqual(t, v, 0)
		require.Less(t, v, 4)
	}
}

func TestRouter_SingleShardIdentity(t *testing.T) {
	r := pg.NewRouter(1)
	for _, k := range []string{"a", "b", "c", "anything"} {
		require.Equal(t, 0, r.Physical(k), "M=1 ⇒ every key resolves to shard 0")
	}
}

func TestRouter_SpreadsAcrossShards(t *testing.T) {
	r := pg.NewRouter(4)
	seen := map[int]int{}
	for i := range 1000 {
		seen[r.Physical(fmt.Sprintf("order-%d", i))]++
	}
	require.Len(t, seen, 4, "1000 keys should touch all 4 shards")
	for shard, n := range seen {
		require.Greater(t, n, 100, "shard %d got too few keys (%d) — distribution skew", shard, n)
	}
}
```

Add `"fmt"` to the imports. After the first green run, replace the `TestRouter_DeterministicAcrossProcesses` body's loose checks with the OBSERVED `key → physical` values as hard equalities (e.g. `require.Equal(t, 2, r.Physical("order-1"))`) so the table is frozen — that is the real cross-process guard.

- [ ] **Step 2: Run — verify it fails**

Run: `go test ./platform/storage/pg/ -run TestRouter -count=1`
Expected: FAIL — `undefined: pg.NewRouter`.

- [ ] **Step 3: Implement `sharded_router.go`**

```go
package pg

import "hash/fnv"

// defaultLogicalShards is the FIXED number of logical shards. Keys hash into
// logical shards; logical shards map to physical shards via a static assignment.
// The logical layer is forward-prep for a future resharding mechanism (keys
// never rehash; only the logical→physical assignment would move). It is NOT
// changed at runtime — changing it would re-route every key. See ADR-0019.
const defaultLogicalShards = 256

// Router maps an aggregate key to a physical shard index in [0, m). The hash is
// pinned FNV-1a 64-bit (stdlib hash/fnv) — deterministic and identical across
// processes, unlike maphash (whose per-process random seed would send the same
// key to different shards in different services). A key maps to exactly one
// physical shard for the life of the deployment.
type Router struct {
	logicalShards int
	assign        []int // len == logicalShards; assign[l] ∈ [0, m)
	m             int
}

// NewRouter builds a Router over m physical shards with the default 256 logical
// shards and the canonical assignment assign[l] = l % m (an even spread). m must
// be >= 1.
func NewRouter(m int) *Router {
	if m < 1 {
		m = 1
	}
	assign := make([]int, defaultLogicalShards)
	for l := range assign {
		assign[l] = l % m
	}
	return &Router{logicalShards: defaultLogicalShards, assign: assign, m: m}
}

// Physical returns the physical shard index for key.
func (r *Router) Physical(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	logical := int(h.Sum64() % uint64(r.logicalShards))
	return r.assign[logical]
}
```

- [ ] **Step 4: Run — capture values, freeze the table, re-run**

Run: `go test ./platform/storage/pg/ -run TestRouter -count=1 -v`
Expected: PASS. Note the printed/observed `r.Physical(...)` values, edit the determinism test to assert them as constants, re-run → still PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/storage/pg/sharded_router.go platform/storage/pg/sharded_router_test.go
git commit -m "feat(pg): sharded Router — pinned FNV-1a routing, 256 logical shards

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `ShardedConfig` + `ShardedPool` construction + `Resolve`

**Files:** Create `platform/storage/pg/sharded_pool.go`; start `platform/storage/pg/sharded_pool_test.go`.

- [ ] **Step 1: Write the failing test** (integration; needs Docker)

```go
package pg_test

import (
	"context"
	"testing"

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

	// Both pools are reachable among the shards.
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
```

- [ ] **Step 2: Run — verify it fails**

Run: `go test ./platform/storage/pg/ -run TestShardedPool_Resolve -count=1`
Expected: FAIL — `undefined: pg.NewSharded` / `pg.ShardedConfig` / `pg.ShardedPool`.

- [ ] **Step 3: Implement `sharded_pool.go`**

```go
package pg

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/platform/config"
)

// ShardedConfig configures a ShardedPool. DSNs lists the writer DSN of each
// physical shard (1..M). ReaderDSNs, when non-empty, must have the same length
// and supplies a reader DSN per shard (a replica); an empty entry inherits the
// shard's writer. PerShard carries the tuning (pool sizes, timeouts, query
// metrics) applied to every shard; its DSN/ReaderDSN fields are ignored (the
// per-shard DSNs above win).
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
	migrateURLs []string // per-shard DSN used by MigrateSharded (MigrateURL if set, else DSN)
}

// NewSharded builds one Pool per DSN (reusing New with PerShard tuning) and a
// Router over len(DSNs). It returns an error and closes any already-built pools
// if a shard fails to connect.
func NewSharded(ctx context.Context, cfg ShardedConfig) (*ShardedPool, error) {
	if len(cfg.DSNs) == 0 {
		return nil, errors.New("pg: NewSharded requires at least one DSN")
	}
	if len(cfg.ReaderDSNs) != 0 && len(cfg.ReaderDSNs) != len(cfg.DSNs) {
		return nil, fmt.Errorf("pg: ReaderDSNs length %d != DSNs length %d", len(cfg.ReaderDSNs), len(cfg.DSNs))
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
```

NB: if `cfg.PerShard.MigrateURL` is set it is applied to ALL shards — which is only correct for `M=1` (one Postgres). For `M>1` each shard needs its own migrate target; in that case leave `PerShard.MigrateURL` empty and the per-shard `DSN` is used. This matches the examples (B/C/D) wiring, which sets `MigrateURL` only in the single-pool/PgBouncer case. Document this in the `ShardedConfig.PerShard` doc comment.

- [ ] **Step 4: Run — verify pass**

Run: `go test ./platform/storage/pg/ -run TestShardedPool_Resolve -count=1 -p 1 && go test ./platform/storage/pg/ -run TestShardedPool_SingleShardIsOnePool -count=1 -p 1`
Expected: PASS (boots 2 then 1 containers).

- [ ] **Step 5: Commit**

```bash
git add platform/storage/pg/sharded_pool.go platform/storage/pg/sharded_pool_test.go
git commit -m "feat(pg): ShardedPool construction + Resolve (one Pool per shard, reuses New)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `ForEachShard` + `HealthCheck`

**Files:** Modify `platform/storage/pg/sharded_pool.go`; extend `sharded_pool_test.go`.

- [ ] **Step 1: Write the failing test**

```go
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
```

Add `"errors"` and `"sync"` to the test imports.

- [ ] **Step 2: Run — verify it fails**

Run: `go test ./platform/storage/pg/ -run 'TestShardedPool_ForEachShard|TestShardedPool_HealthCheck' -count=1`
Expected: FAIL — `sp.ForEachShard`/`sp.HealthCheck` undefined.

- [ ] **Step 3: Implement (append to `sharded_pool.go`)**

```go
import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go-boilerplate/platform/config"
)

// ForEachShard runs fn against every physical shard CONCURRENTLY and returns a
// joined error naming each failing shard. It is the fan-out primitive for
// keyless operations (global LIST, audit verify) and for sharded migrations. A
// partial failure is always surfaced — never a silent partial result.
func (sp *ShardedPool) ForEachShard(ctx context.Context, fn func(idx int, p *Pool) error) error {
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
```

(Merge the new imports into the existing import block — add `"sync"`.)

- [ ] **Step 4: Run — verify pass**

Run: `go test ./platform/storage/pg/ -run 'TestShardedPool_ForEachShard|TestShardedPool_HealthCheck' -count=1 -p 1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/storage/pg/sharded_pool.go platform/storage/pg/sharded_pool_test.go
git commit -m "feat(pg): ShardedPool ForEachShard fan-out + all-up HealthCheck

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: context seam — `WithShardKey`, sharded `RunInTx`/`FromContext`

**Files:** Create `platform/storage/pg/sharded_tx.go`; extend `sharded_pool_test.go`.

- [ ] **Step 1: Write the failing test**

```go
func TestShardedPool_RunInTx_RoutesByKeyAndIsolates(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	sp := newShardedTestPool(t, 2)
	// A tiny table on every shard.
	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		_, err := p.Writer().Exec(ctx, `create table k (v text primary key)`)
		return err
	}))

	const key = "order-77"
	kctx := pg.WithShardKey(ctx, key)
	// Write through the sharded tx; FromContext inside the tx must see the tx.
	require.NoError(t, sp.RunInTx(kctx, func(ctx context.Context) error {
		_, err := sp.FromContext(ctx).Exec(ctx, `insert into k (v) values ($1)`, "hello")
		return err
	}))

	// The row is on Resolve(key) and on NO other shard.
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
		_, _ = sp.FromContext(ctx).Exec(ctx, `insert into k (v) values ('x')`)
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
```

- [ ] **Step 2: Run — verify it fails**

Run: `go test ./platform/storage/pg/ -run 'TestShardedPool_RunInTx|TestShardedPool_NoShardKey' -count=1`
Expected: FAIL — `pg.WithShardKey` / `sp.RunInTx` / `sp.FromContext` undefined.

- [ ] **Step 3: Implement `sharded_tx.go`**

```go
package pg

import (
	"context"
	"errors"
)

type shardKeyCtxKey struct{}

// WithShardKey binds the aggregate shard key to ctx. ShardedPool.RunInTx /
// FromContext / FromContextRead route to Resolve(key). Consumers set it from the
// Kafka record key; the gateway sets it from the aggregate id at the edge.
func WithShardKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, shardKeyCtxKey{}, key)
}

func shardKeyFrom(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(shardKeyCtxKey{}).(string)
	return k, ok && k != ""
}

// errNoShardKey is returned by keyed operations when ctx carries no shard key.
// Fail-closed: a missing key is a wiring bug, never a silent shard-0 write.
var errNoShardKey = errors.New("pg: no shard key in context (use WithShardKey before a sharded operation)")

// RunInTx resolves the shard from the ctx shard key and runs fn in a transaction
// on that shard, delegating to the single-pool RunInTx. The ambient-tx context
// key is shared with RunInTx, so FromContext inside fn observes the transaction.
func (sp *ShardedPool) RunInTx(ctx context.Context, fn func(ctx context.Context) error) error {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return errNoShardKey
	}
	return RunInTx(ctx, sp.Resolve(key), fn)
}

// FromContext returns the ambient transaction if one is open, else the writer of
// the shard resolved from the ctx shard key. Fail-closed when no key is set.
// It only needs the key to pick the pool: the single-pool FromContext already
// returns the ambient tx when present and the pool otherwise. Within one request
// the shard key is stable, so Resolve(key) returns the same pool the open tx was
// started on, and the tx is returned correctly.
func (sp *ShardedPool) FromContext(ctx context.Context) (DBTX, error) {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return nil, errNoShardKey
	}
	return FromContext(ctx, sp.Resolve(key)), nil
}

// FromContextRead is the reader-pool variant of FromContext.
func (sp *ShardedPool) FromContextRead(ctx context.Context) (DBTX, error) {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return nil, errNoShardKey
	}
	return FromContextRead(ctx, sp.Resolve(key)), nil
}
```

- [ ] **Step 4: Run — verify pass**

Run: `go test ./platform/storage/pg/ -run 'TestShardedPool_RunInTx|TestShardedPool_NoShardKey' -count=1 -p 1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/storage/pg/sharded_tx.go platform/storage/pg/sharded_pool_test.go
git commit -m "feat(pg): ShardedPool tx seam — WithShardKey + key-routed RunInTx/FromContext (fail-closed)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `MigrateSharded`

**Files:** Create `platform/storage/pg/sharded_migrate.go`; extend `sharded_pool_test.go`.

- [ ] **Step 1: Write the failing test**

Use the existing gateway/order migration FS pattern — but to keep this a pure-pg test with no example import, ship a tiny in-test migration via `testing/fstest`:

```go
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

	// The table exists on every shard.
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
```

Add `"testing/fstest"` to the imports.

- [ ] **Step 2: Run — verify it fails**

Run: `go test ./platform/storage/pg/ -run TestMigrateSharded -count=1`
Expected: FAIL — `pg.MigrateSharded` undefined.

- [ ] **Step 3: Implement `sharded_migrate.go`**

```go
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
```

- [ ] **Step 4: Run — verify pass**

Run: `go test ./platform/storage/pg/ -run TestMigrateSharded -count=1 -p 1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/storage/pg/sharded_migrate.go platform/storage/pg/sharded_pool_test.go
git commit -m "feat(pg): MigrateSharded — goose per shard via ForEachShard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `M=1` byte-identity test

A dedicated test pinning the headline guarantee: a `ShardedPool` over one DSN behaves like the underlying `Pool`.

**Files:** Extend `platform/storage/pg/sharded_pool_test.go`.

- [ ] **Step 1: Write the test**

```go
func TestShardedPool_SingleShard_ByteIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	sp := newShardedTestPool(t, 1)
	require.NoError(t, sp.ForEachShard(ctx, func(_ int, p *pg.Pool) error {
		_, err := p.Writer().Exec(ctx, `create table k (v text)`)
		return err
	}))

	// Every key routes to the one shard; the sharded tx behaves like a plain tx.
	for _, key := range []string{"a", "b", "order-999"} {
		require.Same(t, sp.Shards()[0], sp.Resolve(key))
		kctx := pg.WithShardKey(ctx, key)
		require.NoError(t, sp.RunInTx(kctx, func(ctx context.Context) error {
			db, err := sp.FromContext(ctx)
			require.NoError(t, err)
			_, err = db.Exec(ctx, `insert into k (v) values ($1)`, key)
			return err
		}))
	}
	var n int
	require.NoError(t, sp.Shards()[0].Reader().QueryRow(ctx, `select count(*) from k`).Scan(&n))
	require.Equal(t, 3, n)
}
```

- [ ] **Step 2: Run — verify pass** (it exercises already-built code)

Run: `go test ./platform/storage/pg/ -run TestShardedPool_SingleShard_ByteIdentity -count=1 -p 1`
Expected: PASS.

- [ ] **Step 3: Full package + race**

Run: `go test ./platform/storage/pg/ -run 'Router|ShardedPool|MigrateSharded' -race -count=1 -p 1`
Expected: PASS, no race.

- [ ] **Step 4: Commit**

```bash
git add platform/storage/pg/sharded_pool_test.go
git commit -m "test(pg): ShardedPool M=1 byte-identity guarantee

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: ADR-0019

**Files:** Create `docs/adr/0019-postgres-sharding.md` (confirm the dir/number first).

- [ ] **Step 1: Confirm location** — `ls docs/adr/ | tail -3` (use the real ADR directory and the next free number; adjust the filename if it differs).

- [ ] **Step 2: Write the ADR** — follow the format of an existing ADR (open `docs/adr/0018-*.md` and mirror its headings: Status / Context / Decision / Consequences). Content must cover:
  - **Status:** Accepted.
  - **Context:** the measured single-Postgres ceiling (~40k trivial inserts/s, ~20k batched audit rows/s, ~5–6k order-tx/s); Tier-2 removed cross-aggregate serialization but the single writer remains the wall; horizontal sharding is the linearity lever (5k = ceil(5000/K) shards).
  - **Decision:** `pg.ShardedPool`, opt-in, `[]*pg.Pool` + `Router`; pinned FNV-1a 64-bit (not maphash — cross-process determinism); 256 fixed logical shards → static physical assignment; routing key = aggregate id carried in ctx (= the Kafka record key in the choreography); no cross-shard tx (aggregate + its outbox/inbox/audit on one shard); keyless ops fan out via `ForEachShard`; `M=1` byte-identical.
  - **Consequences / deliberate boundaries (the honest section):**
    - **Static `M`** pinned at deploy; **live resharding deferred** (changing `M` needs data movement + cutover coordination) — 256 logical shards are the forward-prep so keys never rehash.
    - **Known limitations / production hardening:** readiness = all-shards-up (vs per-aggregate availability); the logical→physical assignment is a single source of truth every service must share; fan-out multiplies connection usage (M× pools per process unless deployed instance-per-shard-group); schema changes must be **expand-contract** because they run per shard, non-atomically across the fleet.
  - **References:** ADR-0017 (sharded relay), ADR-0018 (sharded audit), this spec path.

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0019-postgres-sharding.md
git commit -m "docs(adr): 0019 Postgres sharding — pg.ShardedPool, static M, deliberate boundaries

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes

- **Spec coverage:** Router/pinned-hash → Task 1; ShardedPool+Resolve+Close → Task 2; ForEachShard+HealthCheck → Task 3; ctx seam (WithShardKey, RunInTx, FromContext/Read, fail-closed) → Task 4; MigrateSharded → Task 5; M=1 byte-identity → Task 6 (+ asserted in 1,2); ADR-0019 (static-M honesty + known limitations) → Task 7. ShardedConfig (DSNs/ReaderDSNs/PerShard, additive, `Config` unchanged) → Task 2.
- **Invariants:** no cross-shard tx (RunInTx resolves ONE pool per key); `pg.Pool` untouched (all new files); `M=1` identity (Tasks 2,6); deterministic hash (Task 1 frozen table); fail-closed (Task 4).
- **Type consistency:** `NewRouter(m)`, `Router.Physical(key)`, `NewSharded(ctx, ShardedConfig)`, `ShardedPool.{Resolve,Shards,Len,ForEachShard,HealthCheck,Close,RunInTx,FromContext,FromContextRead}`, `WithShardKey(ctx,key)`, `MigrateSharded(ctx,sp,fsys,dir)` — names identical across tasks.
- **`PG_SHARDS` env parsing** is intentionally NOT in this sub-project — `ShardedConfig` is constructed in code here; the env shape is wired in B/C/D where the example services build it. Flagged, not a gap.
