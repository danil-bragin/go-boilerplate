# Durable audit — staging + drain worker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **After EACH commit run `git log --oneline -1` and confirm the NEW sha — the pre-commit hook (build+fmt+lint) is strict and WILL reject unused params / `fmt.Errorf`-without-args. Fix and re-commit until it lands. Never report a commit done without verifying.**

**Goal:** Add a third audit mode — Durable — that never drops (unlike A2 Eventual) yet keeps the hash-chain `FOR UPDATE` off the command hot path (unlike Strong): the command writes a cheap durable intent in its own tx, and a single-active per-shard worker applies intents to the chain asynchronously and exactly-once.

**Architecture:** The outbox pattern applied to audit, locally (no Kafka — `audit_log` is per-service). A new `audit_pending` staging table; `audit.DurableAudit` behavior inserts an intent in the command tx; `(*PgStore).DrainPending` applies+deletes pending rows per chain in one tx via the existing `RecordBatchSameChain`; `servicekit.AddAuditDrain` runs the drain single-active per physical shard (like the relay).

**Tech Stack:** Go 1.26, pgx v5, goose, the existing `audit.PgStore` (chain hashing, `RecordBatchSameChain`, `chainIDFor`), `pg.RunAsLeader`, testcontainers.

**Spec:** `docs/superpowers/specs/2026-06-13-durable-audit-design.md`.

**Hard rules (every task preserves):**
1. No loss: a committed Durable command's intent is in `audit_pending` (its tx) — applied or pending, never dropped.
2. Exactly-once apply: apply+delete in ONE tx; drain single-active per shard (the chain must be applied serially per chain).
3. Tamper-evidence: same `computeEntryHash`/`RecordBatchSameChain`; `VerifyChain` passes; hashed `at` = original event time (µs-truncated), not apply time.
4. Strong (`audit.Audit`) and Eventual (`audit.AsyncAudit`) are NOT modified — Durable is additive, opt-in.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `platform/security/audit/migrations/00006_audit_pending.sql` | staging table (for audit pkg tests) | Create |
| `platform/security/audit/pending.go` | `InsertPending`, `DrainPending` on `PgStore` | Create |
| `platform/security/audit/pending_test.go` | durability/exactly-once/order tests | Create |
| `platform/security/audit/behavior.go` | add `DurableAudit[C,R]` | Modify |
| `platform/security/audit/metrics.go` | `audit.pending_backlog` gauge | Modify |
| `platform/servicekit/audit_drain.go` | `AddAuditDrain` (per-shard single-active worker) | Create |
| `examples/orders/internal/migrations/sql/000NN_audit_pending.sql` | demo service staging table | Create |
| `platform/security/audit/sharding_bench_test.go` | add a "durable" row to the bench | Modify |

NB: the audit-package tests embed `migrations/*.sql` (see `audit_test.go`), so the new `00006_audit_pending.sql` is picked up automatically. Only the orders service gets the inline migration here (the demo + bench target); other services add the identical migration when they adopt Durable — documented in the spec, not built now.

---

## Task 1: `audit_pending` migration

**Files:** Create `platform/security/audit/migrations/00006_audit_pending.sql`.

- [ ] **Step 1: write the migration**

```sql
-- +goose Up
-- Durable-audit staging (see docs/superpowers/specs/2026-06-13-durable-audit-design.md).
-- A Durable command inserts an audit INTENT here inside its own transaction
-- (cheap, no chain-head lock); a single-active per-shard worker drains these
-- into the append-only audit_log hash chain exactly-once. Owned by the app role:
-- unlike audit_log (append-only, audit_admin-owned), this is transient staging
-- the app must INSERT and the drain must DELETE.
create table audit_pending (
    id         bigserial   primary key,
    chain_id   smallint    not null,
    actor      text        not null,
    action     text        not null,
    subject    text        not null,
    metadata   jsonb,
    created_at timestamptz not null
);
create index audit_pending_drain_idx on audit_pending (chain_id, id);

-- +goose Down
drop table audit_pending;
```

- [ ] **Step 2: confirm it applies** — run any existing audit integration test (which runs all migrations): `go test ./platform/security/audit/ -run TestVerifyChain -count=1 -p 1`. Expected: PASS (migrations incl. 00006 apply clean).
- [ ] **Step 3: commit**
```bash
git add platform/security/audit/migrations/00006_audit_pending.sql
git commit -m "feat(audit): audit_pending staging table migration

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `InsertPending` + `DrainPending` on `PgStore`

**Files:** Create `platform/security/audit/pending.go`; create `platform/security/audit/pending_test.go`. Read `platform/security/audit/audit.go` first for the EXACT internals: `PgStore` fields (`pool`, `chainKey`, `chainShards`, `onError`), `chainIDFor(actor) int16`, `RecordBatchSameChain(ctx, int16, []Entry) error`, `marshalMetadata`, the `at` normalisation (`at.Truncate(time.Microsecond)`, zero→`time.Now().UTC()`), the `Entry` struct, and `pg.FromContext(ctx, s.pool)`.

- [ ] **Step 1: write the failing tests** (`pending_test.go`, package `audit_test`)

```go
func TestInsertPending_StagesWithoutTouchingChain(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool)
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return store.InsertPending(ctx, audit.Entry{Actor: "u1", Action: "order:create", Subject: "o1"})
	}))
	var pending, chain int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_pending`).Scan(&pending))
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_log`).Scan(&chain))
	require.Equal(t, 1, pending, "intent staged")
	require.Equal(t, 0, chain, "chain NOT touched at command time")
}

func TestDrainPending_AppliesExactlyOnceAndVerifies(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(4))
	const n = 200
	for i := range n {
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return store.InsertPending(ctx, audit.Entry{
				Actor: fmt.Sprintf("u%d", i%8), Action: "order:create", Subject: fmt.Sprintf("o%d", i),
			})
		}))
	}
	// Drain until empty.
	total := 0
	for {
		applied, err := store.DrainPending(ctx, 64)
		require.NoError(t, err)
		total += applied
		if applied == 0 {
			break
		}
	}
	require.Equal(t, n, total)
	var pending, chain int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_pending`).Scan(&pending))
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_log`).Scan(&chain))
	require.Equal(t, 0, pending, "all pending applied + deleted")
	require.Equal(t, n, chain, "every intent in the chain exactly once")

	// Draining again is a no-op (exactly-once).
	applied, err := store.DrainPending(ctx, 64)
	require.NoError(t, err)
	require.Equal(t, 0, applied)

	res, err := store.VerifyChain(ctx, time.Time{})
	require.NoError(t, err)
	require.True(t, res.OK, "drained chain must verify; break id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, n, res.Verified)
}
```

Add imports (`context`, `fmt`, `testing`, `time`, audit, pg, require). `newPool` is the audit-package test helper.

- [ ] **Step 2: run → FAIL** (`InsertPending`/`DrainPending` undefined).
`go test ./platform/security/audit/ -run 'TestInsertPending|TestDrainPending' -count=1`

- [ ] **Step 3: implement `pending.go`** (adapt to the real internals you read)

```go
package audit

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// InsertPending stages an audit intent for the Durable mode: a single cheap
// INSERT into audit_pending on the AMBIENT transaction (the command's tx), so it
// commits atomically with the command and never touches the chain-head lock. The
// chain_id is resolved now (chainIDFor(actor)); created_at is the original event
// time (µs-truncated — the value that will be hashed when the drainer applies
// it). A single-active per-shard worker later applies these via DrainPending.
func (s *PgStore) InsertPending(ctx context.Context, e Entry) error {
	metaJSON, err := marshalMetadata(e.Metadata)
	if err != nil {
		return fmt.Errorf("audit: marshal metadata: %w", err)
	}
	at := e.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.Truncate(time.Microsecond)
	db := pg.FromContext(ctx, s.pool)
	if _, err := db.Exec(ctx,
		`insert into audit_pending (chain_id, actor, action, subject, metadata, created_at)
		 values ($1,$2,$3,$4,$5,$6)`,
		s.chainIDFor(e.Actor), e.Actor, e.Action, e.Subject, metaJSON, at); err != nil {
		return fmt.Errorf("audit: stage pending: %w", err)
	}
	return nil
}

type pendingRow struct {
	id    int64
	entry Entry
}

// DrainPending applies up to batchSize staged intents to the hash chain and
// deletes them, exactly-once: it reads pending rows ordered by (chain_id, id),
// groups them by chain, and for each chain group runs RecordBatchSameChain +
// DELETE in ONE transaction (apply+delete atomic). Returns the number applied;
// 0 means the staging table is empty. MUST be called single-active per shard
// (the chain is applied strictly in id order per chain).
func (s *PgStore) DrainPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 128
	}
	rows, err := s.pool.Reader().Query(ctx,
		`select id, chain_id, actor, action, subject, metadata, created_at
		   from audit_pending order by chain_id, id limit $1`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("audit: read pending: %w", err)
	}
	byChain := map[int16][]pendingRow{}
	order := []int16{}
	for rows.Next() {
		var (
			id      int64
			chainID int16
			e       Entry
			meta    []byte
			at      time.Time
		)
		if err := rows.Scan(&id, &chainID, &e.Actor, &e.Action, &e.Subject, &meta, &at); err != nil {
			rows.Close()
			return 0, fmt.Errorf("audit: scan pending: %w", err)
		}
		e.At = at
		e.Metadata = unmarshalMetadata(meta) // add this helper (mirror marshalMetadata); or inline json.Unmarshal into map[string]string when meta != nil
		if _, seen := byChain[chainID]; !seen {
			order = append(order, chainID)
		}
		byChain[chainID] = append(byChain[chainID], pendingRow{id: id, entry: e})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("audit: iterate pending: %w", err)
	}

	applied := 0
	for _, chainID := range order {
		group := byChain[chainID]
		entries := make([]Entry, len(group))
		ids := make([]int64, len(group))
		for i, r := range group {
			entries[i] = r.entry
			ids[i] = r.id
		}
		if err := pg.RunInTx(ctx, s.pool, func(ctx context.Context) error {
			if err := s.RecordBatchSameChain(ctx, chainID, entries); err != nil {
				return err
			}
			_, err := pg.FromContext(ctx, s.pool).Exec(ctx, `delete from audit_pending where id = any($1)`, ids)
			return err
		}); err != nil {
			return applied, fmt.Errorf("audit: drain chain %d: %w", chainID, err)
		}
		applied += len(group)
	}
	return applied, nil
}
```

NB: confirm whether a `marshalMetadata`/un-marshal pair exists. `marshalMetadata` exists; for the read side add a small `unmarshalMetadata([]byte) map[string]string` in pending.go (nil/empty → nil; else `json.Unmarshal`). Critical: the `at` round-trips through timestamptz (µs) — it was truncated on insert, so the hash computed by `RecordBatchSameChain` (which also truncates) matches; do NOT re-truncate differently. Confirm `RecordBatchSameChain` uses `e.At` when non-zero (it does — it normalises zero→now then truncates; a non-zero `at` from the pending row is used as-is after truncation, identical value). Verify this in audit.go and adjust if `RecordBatchSameChain` would overwrite a provided `at`.

- [ ] **Step 4: run → PASS.** `go test ./platform/security/audit/ -run 'TestInsertPending|TestDrainPending' -count=1 -p 1`
- [ ] **Step 5: commit** (`feat(audit): InsertPending + DrainPending — durable staging applied exactly-once`). Verify SHA.

---

## Task 3: `DurableAudit` behavior

**Files:** Modify `platform/security/audit/behavior.go`; test alongside in `pending_test.go`. Read the existing `Audit` and `AsyncAudit` in behavior.go (actor source `actorFrom`, tenant metadata, run-handler-then-record ordering).

- [ ] **Step 1: failing test** (add to `pending_test.go`)

```go
func TestDurableAudit_StagesAfterSuccess_NotOnError(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool)
	b := audit.DurableAudit[string, string](store, "order:create", func(c string) string { return c })

	// Success path runs inside a tx (Durable requires Transactional) → intent staged.
	h := b(func(ctx context.Context, c string) (string, error) { return "ok", nil })
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_, err := h(ctx, "o1")
		return err
	}))
	var pending int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_pending`).Scan(&pending))
	require.Equal(t, 1, pending)

	// Handler error → nothing staged (and the tx rolls back anyway).
	wantErr := errors.New("boom")
	hErr := b(func(ctx context.Context, c string) (string, error) { return "", wantErr })
	_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_, err := hErr(ctx, "o2")
		return err
	})
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_pending`).Scan(&pending))
	require.Equal(t, 1, pending, "no intent on handler error")
}
```

(`errors` import.)

- [ ] **Step 2: run → FAIL** (`DurableAudit` undefined).

- [ ] **Step 3: implement in `behavior.go`** (mirror `Audit`'s Entry construction exactly; sink = `InsertPending`)

```go
// DurableAudit is the durable third audit mode (see the durable-audit design):
// after the handler succeeds it stages the same Entry as Audit into audit_pending
// on the command's transaction (a cheap insert, NO chain-head lock), and a
// single-active per-shard drain worker applies it to the hash chain
// asynchronously and exactly-once. Unlike AsyncAudit it NEVER drops (the intent
// is committed with the command); unlike Audit it does not hold the chain lock on
// the hot path (so it lifts the Strong order-create ceiling). It REQUIRES a
// Transactional command — the intent must commit atomically with the command; a
// staging error rolls the command back (the durability contract).
func DurableAudit[C, R any](store *PgStore, action string, subjectFor func(C) string) cqrs.Behavior[C, R] {
	return func(next cqrs.HandlerFunc[C, R]) cqrs.HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			res, err := next(ctx, cmd)
			if err != nil {
				return res, err
			}
			entry := Entry{Actor: actorFrom(ctx), Action: action, Subject: subjectFor(cmd)}
			if tid, ok := tenant.FromContext(ctx); ok {
				entry.Metadata = map[string]string{"tenant_id": tid}
			}
			if perr := store.InsertPending(ctx, entry); perr != nil {
				var zero R
				return zero, perr
			}
			return res, nil
		}
	}
}
```

- [ ] **Step 4: run → PASS.** `go test ./platform/security/audit/ -run TestDurableAudit -count=1 -p 1`
- [ ] **Step 5: commit** (`feat(audit): DurableAudit behavior — stage intent in command tx, never drops`). Verify SHA.

---

## Task 4: `audit.pending_backlog` metric

**Files:** Modify `platform/security/audit/metrics.go`. Read it (the otel meter `security.audit`, the nil-degrading counter pattern from A2).

- [ ] **Step 1: add an observable gauge.** Add to `writerMetrics` (or a new small metrics holder) an `Int64ObservableGauge` `audit.pending_backlog` registered with a callback the drain worker can feed, OR (simpler, no callback plumbing) a method `recordBacklog(ctx, n int64)` on a gauge the worker calls each tick. Use the simpler form: an `Int64Gauge` (otel `Int64Gauge`, available in the SDK) recorded each drain tick. Match the existing nil-degrading idiom:

```go
// in newWriterMetrics() or a sibling constructor:
if g, err := m.Int64Gauge("audit.pending_backlog",
	metric.WithDescription("Rows in audit_pending awaiting the durable-audit drain (lag indicator)")); err == nil {
	wm.backlog = g
}
// method:
func (m writerMetrics) recordBacklog(ctx context.Context, n int64) {
	if m.backlog != nil {
		m.backlog.Record(ctx, n)
	}
}
```

If `Int64Gauge` is not in the pinned otel version, use an `Int64ObservableGauge` with a registered callback that reads an atomic the worker updates. Pick whichever the pinned otel SDK supports (check `go.opentelemetry.io/otel/metric` version); document the choice.

- [ ] **Step 2: build + vet** `go build ./platform/security/audit/ && go vet ./platform/security/audit/`. (Exercised in Task 5's worker; no standalone test needed — note it.)
- [ ] **Step 3: commit** (`feat(audit): audit.pending_backlog gauge`). Verify SHA.

---

## Task 5: `servicekit.AddAuditDrain` — per-shard single-active worker

**Files:** Create `platform/servicekit/audit_drain.go`. Read `platform/servicekit/relay.go` (the per-shard loop + per-shard single-active leader wiring added in Tier-3 B4 — `s.shards.Shards()`, the shard-distinct lock key, `pg.RunAsLeader` / `outbox.WithSingleActive`), `platform/servicekit/periodic.go` (`AddPeriodicWorker(name, interval, jitter, singleActive, fn)`), `platform/servicekit/audit.go` (`AddAuditWriter` lifecycle pattern), `platform/storage/pg/leader.go` (`RunAsLeader(ctx, *pgxpool.Pool, name, fn)`).

- [ ] **Step 1: implement `AddAuditDrain`.** It registers, PER physical shard, a single-active periodic worker that builds a per-shard `audit.PgStore` (same chain key + chain shards as the service's audit config) and calls `DrainPending` each tick, recording the backlog. Mirror the relay's per-shard leader wiring (B4): leader lock on the shard's writer pool with a shard-distinct name (e.g. `audit_drain:<schema>` + `:pshard:<i>` only when `Len()>1`, like the relay). At M=1 one worker.

```go
package servicekit

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/security/audit"
)

// AddAuditDrain wires the durable-audit drain: a single-active (leader-elected)
// periodic worker PER physical shard that applies staged audit intents
// (audit_pending) to the hash chain via DrainPending. Single-active because a
// hash chain must be applied serially per chain. Build the per-shard audit store
// with the SAME chain key/shards as the command-side store. Must be called
// before Start. interval<=0 disables it.
func (s *Service) AddAuditDrain(interval time.Duration, batchSize int, opts ...audit.Option) error {
	if interval <= 0 {
		return nil
	}
	if s.started {
		return fmt.Errorf("servicekit: AddAuditDrain called after Start — the worker would never run")
	}
	for i, p := range s.shards.Shards() {
		shardPool := p
		idx := i
		store := audit.NewPgStore(shardPool, opts...)
		name := "audit_drain:" + s.cfg.PG.SchemaOrDefault() // confirm how the schema/name is derived; relay uses current_schema — match its convention
		if s.shards.Len() > 1 {
			name = fmt.Sprintf("%s:pshard:%d", name, idx)
		}
		s.goroutines = append(s.goroutines, func(ctx context.Context) {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					// Leader-gated: only the elected instance drains this shard.
					_ = runAuditDrainAsLeader(ctx, shardPool, name, store, batchSize)
				}
			}
		})
	}
	return nil
}
```

ADAPT to the REAL leader mechanism (read relay.go/B4): the relay uses a leader-elected loop, not a per-tick `RunAsLeader`. Prefer mirroring the relay's exact pattern — a single `pg.RunAsLeader(ctx, shardPool.Writer(), name, drainLoop)` where `drainLoop` ticks internally and drains while leader (so leadership is held across ticks, not re-acquired each tick). Implement `runAuditDrainAsLeader`/`drainLoop` to match the relay's structure. The worker drains in a loop until empty each tick (`for { applied,_ := store.DrainPending(ctx, batchSize); record backlog; if applied==0 break }`) then waits the interval. On `DrainPending` error: log via `s.logger` + leave rows pending (retried) — no loss.

CONFIRM the schema/lock-name derivation against relay.go so the audit-drain lock never collides with the relay's lock (different prefix `audit_drain:` vs `outbox_relay:` — fine).

- [ ] **Step 2: build + servicekit tests + M=1 boot.** `go build ./... && go test ./platform/servicekit/ -count=1 -p 1`. Add a focused test if the relay has a white-box test pattern to mirror (e.g. assert one drain goroutine registered per shard, shard-distinct lock name at M>1); otherwise rely on build + the audit-package drain tests + note it.
- [ ] **Step 3: commit** (`feat(servicekit): AddAuditDrain — per-shard single-active durable-audit worker`). Verify SHA.

---

## Task 6: orders demo migration + bench row

**Files:** Create `examples/orders/internal/migrations/sql/000NN_audit_pending.sql` (next free number — confirm with `ls examples/orders/internal/migrations/sql/`); modify `platform/security/audit/sharding_bench_test.go`.

- [ ] **Step 1: orders migration** — the same `audit_pending` table as Task 1 (copy the SQL verbatim; orders is the demo/bench service that can opt into Durable). Confirm the next migration number.
- [ ] **Step 2: extend the gated bench** (`sharding_bench_test.go`) — add a `measureDurable` mirroring `measureStrong` but the keyed tx does { insert orders_t + insert outbox_t + `store.InsertPending(ctx, Entry)` } (cheap intent, NO chain lock), then DRAIN to empty (`for { applied,_ := store.DrainPending(ctx, 256); ... }`), then assert orders==outbox==total AND per-shard VerifyChain OK + sum Verified==total. Add a "durable" row to the result table at 1 shard (and optionally 2/4). The expected story: Durable command-rate >> Strong (chain-lock off the hot path), audit fully applied + verified, zero loss. Keep it under the existing `TIER3_BENCH=1` gate (or a new `DURABLE_BENCH=1` — reuse `TIER3_BENCH`). The `buildShards` helper already migrates the audit FS (now incl. 00006) + creates orders_t/outbox_t — confirm 00006 is in the embed so audit_pending exists on each shard.
- [ ] **Step 3: run** `TIER3_BENCH=1 go test ./platform/security/audit/ -run TestShardingConsistencyBench -count=1 -p 1 -v` — capture the durable vs strong numbers.
- [ ] **Step 4: commit** (`test(audit): durable bench row + orders audit_pending migration — durable >> strong on hot path`). Verify SHA.

---

## Task 7: docs

**Files:** Modify `docs/operations.md` (the consistency-levels section from A2) + `docs/ARCHITECTURE.md`.

- [ ] **Step 1:** operations.md — extend the A2 "Consistency levels" section with the THIRD audit mode (Durable): the table row (durable, never-drops, applied async, intent-in-tx), when to use it (regulatory audit that can't drop but tolerates lag; also lifts the Strong write ceiling), how to wire (`audit.DurableAudit` + `servicekit.AddAuditDrain`), the `audit.pending_backlog` alert (drain lag), and that it requires a Transactional command. ARCHITECTURE.md — add `audit_pending` + the drain worker to the audit component description. Reference the spec.
- [ ] **Step 2: commit** (`docs(audit): durable audit mode — operations + architecture`). Verify SHA.

---

## Self-review notes

- **Spec coverage:** staging migration → T1; InsertPending/DrainPending (exactly-once apply+delete, ordered) → T2; DurableAudit behavior (intent in tx, rollback on stage error, not-on-handler-error) → T3; backlog metric → T4; per-shard single-active drain worker → T5; demo migration + durable bench (no-loss + VerifyChain + throughput vs strong) → T6; docs (third mode) → T7.
- **Hard rules:** no-loss (intent in command tx — T3; survives undrained — T2 test); exactly-once (apply+delete one tx + single-active — T2/T5); tamper-evidence (RecordBatchSameChain + `at` round-trip — T2 VerifyChain); Strong/Eventual untouched (T3 only ADDS DurableAudit).
- **Type consistency:** `InsertPending(ctx, Entry) error`, `DrainPending(ctx, batchSize int) (int, error)`, `DurableAudit[C,R](*PgStore, string, func(C)string) cqrs.Behavior`, `AddAuditDrain(interval, batchSize, ...audit.Option) error`, `chainIDFor → int16`, `RecordBatchSameChain(ctx, int16, []Entry)` — consistent across tasks + match the shipped audit/Tier-3 API.
- **OPEN flags for the implementer:** (a) confirm `RecordBatchSameChain` uses a provided non-zero `Entry.At` as-is (after µs-truncate) so the chain hash matches the intent time — if it overwrites `at`, the durable chain's timestamps would differ from intent and break the "original event time" rule; adjust RecordBatchSameChain to honour a provided `at` if needed (it already truncates; verify it does not force `now`). (b) confirm the otel SDK version supports `Int64Gauge`; else use `Int64ObservableGauge` + atomic. (c) confirm the leader-lock name derivation matches the relay's convention (schema source) so there's no collision and M=1 key is stable.
