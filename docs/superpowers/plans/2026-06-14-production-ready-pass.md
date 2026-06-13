# Production-ready pass — collapse migrations + hygiene + verify — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]`. **After EACH commit run `git log --oneline -1` and confirm the NEW sha — the pre-commit hook (build+fmt+lint) is strict. Never report a commit done without verifying.**

**Goal:** Collapse each service's incremental migration chain into one authoritative `00001_init.sql` (safe — the template was never deployed), proven byte-identical by a schema-diff oracle; then a bounded hygiene sweep; then full green verification.

**Architecture:** Hand-merge each chain into a single baseline preserving the non-obvious conditional logic (audit ownership-transfer, partitioned-outbox-from-day-one, hash-chain + chain_id, genesis seed). A self-contained Go oracle introspects two freshly-migrated databases (OLD chain vs NEW baseline) via the system catalogs and asserts identical schema. The existing migration tests (append_only / chain_shards / partition / doc-test / squawk) stay the behavioural oracle.

**Tech Stack:** Go 1.26, goose (via `pg.Migrate`), pgx v5, testcontainers (`pgtest.NewDSN`), Postgres 16.

**Spec:** `docs/superpowers/specs/2026-06-14-production-ready-pass-design.md`.

**Hard rules:** collapsed schema byte-identical (oracle); behavioural guarantees preserved (existing migration tests pass against the baseline); no deployed DB (document in each baseline header); CI fully green; hygiene changes no behaviour/API.

---

## Task 1: the schema-diff oracle (reusable verification helper)

A throwaway test helper used by every collapse task to prove OLD-chain ≡ NEW-baseline. It introspects two pools via the catalogs (no `pg_dump` binary needed) and compares a normalised fingerprint. Lives in a scratch package so it can read arbitrary migration dirs.

**Files:** Create `scripts/migoracle/oracle.go` + `scripts/migoracle/oracle_test.go` (a throwaway module-local package; deleted in Task 7's cleanup — note it).

- [ ] **Step 1: implement the fingerprint + diff** (`scripts/migoracle/oracle.go`)

```go
// Package migoracle is a throwaway verification tool for the migration-collapse
// production-ready pass (docs/superpowers/specs/2026-06-14-production-ready-pass-design.md).
// It applies an OLD goose migration FS and a NEW one to two fresh databases and
// asserts their schemas are byte-identical via the system catalogs. Delete after
// the collapse lands.
package migoracle

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go-boilerplate/platform/storage/pg"
)

// Fingerprint returns a deterministic, normalised description of the public
// schema of the database behind pool: every table column, index, constraint,
// and partition bound. Two databases with identical fingerprints have identical
// schemas for the purposes of this collapse.
func Fingerprint(ctx context.Context, pool *pg.Pool) (string, error) {
	var b strings.Builder
	q := func(header, sql string) error {
		rows, err := pool.Reader().Query(ctx, sql)
		if err != nil {
			return fmt.Errorf("%s: %w", header, err)
		}
		defer rows.Close()
		var lines []string
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			parts := make([]string, len(vals))
			for i, v := range vals {
				parts[i] = fmt.Sprintf("%v", v)
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		if err := rows.Err(); err != nil {
			return err
		}
		sort.Strings(lines)
		b.WriteString("== " + header + " ==\n")
		b.WriteString(strings.Join(lines, "\n"))
		b.WriteString("\n")
		return nil
	}
	// Columns (table, name, type, nullable, default). Exclude goose's own table.
	if err := q("columns", `
		select table_name, column_name, data_type, is_nullable, coalesce(column_default,'')
		from information_schema.columns
		where table_schema='public' and table_name <> 'goose_db_version'`); err != nil {
		return "", err
	}
	// Indexes (full indexdef).
	if err := q("indexes", `
		select indexdef from pg_indexes
		where schemaname='public' and tablename <> 'goose_db_version'`); err != nil {
		return "", err
	}
	// Constraints (full def).
	if err := q("constraints", `
		select conrelid::regclass::text, conname, pg_get_constraintdef(oid)
		from pg_constraint
		where connamespace = 'public'::regnamespace`); err != nil {
		return "", err
	}
	// Partition bounds (partitioned tables + their partitions).
	if err := q("partitions", `
		select c.relname, pg_get_expr(c.relpartbound, c.oid)
		from pg_class c join pg_inherits i on c.oid = i.inhrelid
		join pg_class p on i.inhparent = p.oid
		where p.relnamespace = 'public'::regnamespace`); err != nil {
		return "", err
	}
	// Partitioned-table strategy (so a plain table vs RANGE-partitioned table differ).
	if err := q("partitioned", `
		select c.relname, pt.partstrat
		from pg_partitioned_table pt join pg_class c on c.oid = pt.partrelid
		where c.relnamespace = 'public'::regnamespace`); err != nil {
		return "", err
	}
	return b.String(), nil
}
```

- [ ] **Step 2: the comparison test harness** (`scripts/migoracle/oracle_test.go`)

```go
package migoracle_test

import (
	"context"
	"os"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/scripts/migoracle"

	"github.com/stretchr/testify/require"
)

// TestCollapseEquivalent applies the OLD chain (dir at OLD_MIG) and the NEW
// baseline (dir at NEW_MIG) to two fresh databases and asserts identical schema.
// Run per service:  OLD_MIG=/tmp/old NEW_MIG=examples/orders/internal/migrations/sql \
//                    go test ./scripts/migoracle/ -run TestCollapseEquivalent -count=1
func TestCollapseEquivalent(t *testing.T) {
	oldDir, newDir := os.Getenv("OLD_MIG"), os.Getenv("NEW_MIG")
	if oldDir == "" || newDir == "" {
		t.Skip("set OLD_MIG and NEW_MIG to two goose migration dirs")
	}
	ctx := context.Background()

	migrate := func(dir string) *pg.Pool {
		dsn := pgtest.NewDSN(t)
		require.NoError(t, pg.Migrate(ctx, dsn, os.DirFS(dir), "."),
			"migrations in %s must apply", dir)
		p, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close(context.Background()) })
		return p
	}

	fpOld, err := migoracle.Fingerprint(ctx, migrate(oldDir))
	require.NoError(t, err)
	fpNew, err := migoracle.Fingerprint(ctx, migrate(newDir))
	require.NoError(t, err)
	require.Equal(t, fpOld, fpNew, "collapsed baseline schema must be byte-identical to the old chain")
}
```

Confirm `pg.Migrate` accepts an `fs.FS` + a dir arg (it does: `Migrate(ctx, dsn, fsys, dir)`); `os.DirFS(dir)` + `"."` points goose at the dir's `*.sql`. If goose needs the files at a subpath, pass `os.DirFS(parent)` + the leaf dir name instead — adapt so it finds the `.sql` files.

- [ ] **Step 3: build** `go build ./scripts/migoracle/ && go vet ./scripts/migoracle/`. Smoke it on an UNCHANGED service to prove the harness works (old==new trivially): `OLD_MIG=examples/notifications/internal/migrations/sql NEW_MIG=examples/notifications/internal/migrations/sql go test ./scripts/migoracle/ -run TestCollapseEquivalent -count=1` → PASS.
- [ ] **Step 4: commit** (`test(mig): schema-diff oracle for migration collapse (throwaway)`). Verify SHA.

---

## Tasks 2–5: collapse each service (one task per service)

Each follows the SAME procedure; the per-service object checklist differs. **Procedure (per service `S` with migration dir `D`):**

1. **Capture the OLD chain** to a temp dir:
   `rm -rf /tmp/oldmig && mkdir -p /tmp/oldmig && cp D/*.sql /tmp/oldmig/`
2. **Read every file in `D`** in order; understand the cumulative final schema.
3. **Write `D/00001_init.sql`** (overwrite the existing 00001) as the merged baseline — Up = all final DDL in dependency order, Down = drop all. Header MUST state: "Collapsed baseline (template never deployed). DO NOT re-collapse a live schema — a deployed DB has applied the prior chain." Preserve the conditional blocks listed per service below.
4. **Delete the other files**: `rm D/00002_*.sql … D/0000N_*.sql` (keep ONLY `00001_init.sql`).
5. **Run the oracle**: `OLD_MIG=/tmp/oldmig NEW_MIG=D go test ./scripts/migoracle/ -run TestCollapseEquivalent -count=1 -p 1` → MUST PASS (identical schema). If it fails, the diff in the failure output names the drifted object — fix `00001_init.sql` and re-run until empty.
6. **Run the service's migration + behaviour tests** (they apply the baseline): `go test ./<service tests> -count=1 -p 1` → green.
7. **squawk** the baseline if the repo lints migrations (the CI "SQL lint (squawk)" job): run the same squawk invocation locally if available, else rely on CI; add a scoped `-- squawk-ignore <rule>` with a comment for any safe-on-empty-DB false positive.
8. **Commit** (`refactor(<svc>): collapse migration chain to a single baseline`). Verify SHA.

### Task 2: gateway (`examples/gateway/internal/migrations/sql/`, 7→1)
Baseline `00001_init.sql` must contain (final shape): the read-model table(s) from the old 00001 (orders_read incl. the created_at index from 00002, pending row table, idempotency table — whatever 00001 created) + product_views (from 00007) + the full audit stack (audit_log in FINAL shape — append-only ownership-transfer do-block + hash-chain columns + chain_id column/index; audit_chain_head genesis seed, no singleton check). Fold the audit_log columns into the initial `create table` (table is app-owned before the transfer), then the conditional ownership-transfer do-block LAST. Read the gateway 00003–00006 audit files for the exact column set + the do-block.

### Task 3: orders (`examples/orders/internal/migrations/sql/`, 6→1)
Baseline must contain: orders domain table; the outbox table in FINAL **partitioned** shape (RANGE by created_at, DEFAULT partition, composite PK `(id, created_at)`, per-partition partial published index, the `topic` column from 00002); inbox dedup table; payment-tracking columns (00003); the full audit stack (append_only 00004 + chain_shards 00005 + audit_pending 00006) in final shape. Preserve the audit ownership-transfer do-block + audit_pending (app-owned).

### Task 4: payments (`examples/payments/internal/migrations/sql/`, 4→1)
Baseline: payments domain table; partitioned outbox (final, incl. topic col from 00002); inbox; audit stack (append_only 00003 + chain_shards 00004) final shape. No audit_pending (payments chain has none — confirm against the files; if absent in the chain, do NOT add it).

### Task 5: platform audit test-embed (`platform/security/audit/migrations/`, 6→1)
This FS is embedded by the audit package's own tests (`//go:embed migrations/*.sql`). Baseline: audit_log final shape (append-only ownership-transfer do-block + actor index from 00002 + hash-chain columns from 00004 + chain_id from 00005); audit_chain_head genesis seed (no singleton); audit_pending (00006). Run the FULL audit package test suite after (append_only, chain_shards, pending, throughput, the durable tests) — they are the strongest behavioural oracle. The collapse MUST keep `TestAuditMigration_TransfersOwnershipWhenAuditAdminExists` green (the conditional transfer) AND the bare-testcontainer tests green (transfer skipped).

> notifications (`examples/notifications/internal/migrations/sql/`, already a single `00001_init.sql`): no collapse needed. Optionally add the "never re-collapse a live schema" header note for consistency — fold into Task 6 hygiene, not its own task.

---

## Task 6: hygiene sweep

**Files:** across `platform/` + `examples/` + `docs/`.

- [ ] **Step 1: dead code / unused exports.** `grep -rn "func WrapPool\|func WrapShards" platform/storage/pg/` then grep their usages across the repo; if an exported helper has ZERO non-test callers and was added speculatively, remove it (and its test). If used, keep. Run `go build ./... && go vet ./...` after. (Do NOT remove anything with real callers.)
- [ ] **Step 2: TODO/FIXME inventory.** `grep -rn "TODO\|FIXME\|XXX" platform/ examples/ --include='*.go' | grep -v '_test.go'`. For each: resolve if trivial, or confirm it's a documented deferral (acceptable). Report the list in the commit; do not leave "implement later" stubs in shipped paths.
- [ ] **Step 3: comment/doc accuracy.** Grep for stale comments that survived this session's changes: `grep -rn "two consistency modes\|two audit modes\|audit_entries" docs/ platform/` (audit is THREE modes now; table is `audit_log`). Fix any. Confirm no doc still references a pre-collapse migration filename (`grep -rn "0000[2-9]_" docs/` — migration refs in docs should now say "the baseline" not a numbered file).
- [ ] **Step 4: doc truth-pass.** In `docs/operations.md`, `docs/ARCHITECTURE.md`, `docs/adding-a-service.md`, `docs/conventions.md`, `README.md`: add/confirm a note that each service ships a SINGLE baseline migration (collapsed; "never re-collapse a live schema"). Ensure adding-a-service's migration guidance says "extend with 00002_*.sql onward" (new features after the baseline). Keep the doc-test code blocks compiling.
- [ ] **Step 5: prod-default review (confirm, no change).** Verify still-wired (read, don't break): `config.RequireProductionSafety` / gateway `Config.Validate` (rejects AuthDisabled/sslmode=disable/CORS=*/fail-open in production); `config.Secret` redaction; fail-closed ratelimit; JWKS https-enforce; append-only audit ownership. If any regressed this session, fix; else note "confirmed wired" in the commit.
- [ ] **Step 6: commit** (`chore: hygiene sweep — dead code, TODO audit, doc truth-pass, prod-default review`). Verify SHA. (Split into multiple commits if the changes are large/independent.)

---

## Task 7: remove the throwaway oracle + final verification

- [ ] **Step 1: remove the oracle scratch package** `rm -rf scripts/migoracle` (its job is done; the permanent guard is the migration test suite). `go build ./...` clean.
- [ ] **Step 2: full verification (the production-ready gate):**
  - `go build ./...` → clean
  - `go vet ./...` → clean
  - `golangci-lint run ./...` → 0 issues
  - `govulncheck ./...` → no findings (or only documented/unfixable)
  - `-race` on concurrency-critical: `go test ./platform/storage/pg/ ./platform/security/audit/ ./platform/messaging/consume/ ./platform/servicekit/ ./platform/messaging/retry/ -race -count=1 -p 1` → clean
  - full suite: `go test ./platform/... -count=1 -p 1` and `go test ./examples/... -count=1 -p 1` → green (re-run any contention-flake in isolation to confirm)
- [ ] **Step 3: commit** (`chore: remove migration-collapse oracle scratch package`). Verify SHA.
- [ ] **Step 4: push + CI.** `git push origin main`; watch the run; confirm ALL jobs green — especially **SQL lint (squawk)** over the new baselines, **doc-test/scaffold/errgen** fast-lane, and the full **Test** job. Fix any CI-only failure (e.g. squawk rejecting a baseline statement → scoped squawk-ignore with a comment) and re-push until green.

---

## Self-review notes

- **Spec coverage:** oracle (W1 verification) → T1; collapse gateway/orders/payments/audit-embed → T2–T5 (notifications folded into T6); hygiene (dead code/TODO/comments/doc/prod-defaults) → T6; verify+CI (build/vet/lint/vuln/race/suite/squawk) → T7. The "byte-identical schema" hard rule is enforced by the T1 oracle invoked in every collapse task's step 5.
- **Behavioural oracle preserved:** each collapse task re-runs the service's migration/behaviour tests (T5 explicitly re-runs the full audit suite incl. the append_only conditional-transfer + durable + chain_shards tests).
- **Conditional-logic preservation** (the collapse foot-gun) is called out per service: audit ownership-transfer do-block kept; partitioned-outbox-from-day-one kept; chain_id/hash-chain folded into the initial create table where app-owned. The oracle catches any drift.
- **No-placeholder:** the oracle is provided complete; per-service tasks give the exact procedure + object checklist; the SQL merge is service-specific and authored by reading the real files (the oracle is the gate, so the merge cannot silently drift).
- **OPEN flags for implementer:** (a) confirm `pg.Migrate` + `os.DirFS(dir)` finds the `.sql` files (adjust the dir arg if goose expects a subpath); (b) confirm payments has NO audit_pending in its chain before deciding whether the baseline includes it; (c) if squawk rejects a baseline `create table ... partition by range` or a `not valid` constraint as "unsafe", add a scoped squawk-ignore — these are safe on an empty/never-deployed DB.
