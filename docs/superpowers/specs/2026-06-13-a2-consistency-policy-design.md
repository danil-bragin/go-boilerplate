# Design — A2: per-command ConsistencyPolicy (granular ACID/BASE)

**Date:** 2026-06-13
**Status:** Approved (brainstorm), pending implementation plan

## Goal

Let each command declare, at compile time, **how strong its consistency is**, so
high-volume low-value flows (clickstream, browse, add-to-cart) can run with
relaxed, eventual guarantees while financial flows (orders, payments) stay fully
ACID. This lifts the measured single-actor audit-chain / commit-fsync ceiling
exactly where volume matters, without weakening the flows that need strength.

Motivation (measured this session): the order-create write path is gated by the
**synchronous audit-in-transaction** (`can't-audit → don't-commit`) — the
`audit_chain_head FOR UPDATE` serialization. Strong consistency is the right
default for a reference template and for money, but applying it uniformly to
high-volume flows is the bottleneck. A2 makes the strength a per-command choice.

## Hard rules

1. **Effectively-once (transactional outbox + inbox) is NEVER relaxed.** A2
   relaxes *observation* consistency (audit completeness, read-your-writes,
   projection lag), not *delivery* (no lost/duplicated events). For commands that
   enqueue to the outbox, the order-write + outbox-enqueue stay atomic.
2. **Strong is the default and stays byte-identical** to today (Phase-1
   batch-apply unaffected). Relaxation is explicit opt-in per command type.
3. **Compile-time, per command type.** A request can never downgrade a Strong
   (money) command at runtime.
4. **tamper-evidence holds.** Async audit still writes the same hash chain;
   `VerifyChain` passes; per-chain order is preserved within each batch.

## The policy

```go
// platform/cqrs/consistency.go
type ConsistencyPolicy struct {
	Transactional  bool // wrap the handler in the cqrs Transaction behavior (ACID). Default true.
	SyncAudit      bool // audit inside the command tx (can't-audit→don't-commit). Default true.
	SyncRYW        bool // synchronous pending-row insert (read-your-writes). Default true.
	SyncProjection bool // sync per-event projection (vs best-effort). Default true.
}

var Strong   = ConsistencyPolicy{true, true, true, true}
var Eventual = ConsistencyPolicy{Transactional: true, SyncAudit: false, SyncRYW: false, SyncProjection: false}

// Override one axis: Strong.With(SyncAudit(false)), Eventual.With(Transactional(false)), …
func (p ConsistencyPolicy) With(opts ...Option) ConsistencyPolicy
```

- `Eventual` **keeps `Transactional: true`** — atomicity (and outbox
  effectively-once) is preserved by default; only audit/RYW/projection relax.
- `Transactional: false` is an **explicit opt-in** for single-write / no-outbox
  commands. It is a foot-gun otherwise (see Guard).

### Axis semantics

| Axis | `true` (strong, default) | `false` (eventual) |
|---|---|---|
| `Transactional` | handler runs inside `cqrs.Transaction` (`pg.RunInTx`) | no cqrs-level wrapping tx; statements auto-commit. **Only safe for ≤1 write and no outbox.** NB: Kafka-consumed commands still get the structural `inbox.ProcessOnce` tx (dedup) — this axis governs the *cqrs* Transaction behavior, not the inbox tx. |
| `SyncAudit` | `audit.Audit` behavior records in the command tx; a Record error rolls the command back | the audit `Entry` is handed to the async `BufferedAuditWriter` after the handler returns; the command does NOT wait and does NOT roll back on audit failure (best-effort) |
| `SyncRYW` | gateway POST writes the pending row synchronously before responding | uses the existing `GATEWAY_PENDING_ASYNC` batched path; GET right after POST may briefly 404 |
| `SyncProjection` | projection applies each event in its own committed tx (today's behaviour, incl. Phase-1 batch) | projection writes are best-effort/deferred for that view (read-model lag grows) |

## Components

### 1. `platform/cqrs/consistency.go` (new)
`ConsistencyPolicy`, `Strong`/`Eventual` presets, `Option`s, and
`Pipeline[C,R].WithConsistency(p ConsistencyPolicy)`. Wiring effect:
- `p.Transactional` gates whether the pipeline includes the `Transaction`
  behaviour (today `WithTransaction(pool)` adds it explicitly).
- `p.SyncAudit` selects `audit.Audit` (sync, in-tx) vs `audit.AsyncAudit` (a thin
  behaviour that enqueues to the `BufferedAuditWriter` after the handler).

### 2. `platform/security/audit/asyncwriter.go` (new — the main new subsystem)
`BufferedAuditWriter`:
- `Enqueue(Entry) bool` — non-blocking; pushes onto a bounded channel; returns
  false (and increments a dropped counter) when the buffer is full.
- A background drain goroutine: collects up to `BatchSize` entries (or a flush
  tick), **groups them by `chain_id`** (`store.chainIDFor(actor)`), and writes
  each chain-group in ONE transaction reusing the existing sharded-chain Record
  logic — amortising the `audit_chain_head FOR UPDATE` across the batch (the
  Phase-1 batch pattern, applied to audit). Per-chain order within a batch is the
  enqueue order.
- Bounded buffer; overflow → drop + `audit.dropped_total` counter + an
  operations alert. Crash loses un-drained buffer entries (best-effort — the
  documented trade for Eventual flows).
- Lifecycle: started/stopped via `servicekit.AddAuditWriter` (a periodic/worker
  goroutine under the harness; drains on shutdown with a bounded grace).
- `audit.AsyncAudit[C,R](writer, action, subjectFn)` — the cqrs behaviour that
  builds the Entry (same as `audit.Audit`) and `Enqueue`s it after the handler
  succeeds, never failing the command.

### 3. Gateway wiring
RYW already has `GATEWAY_PENDING_ASYNC` (reuse). Projection best-effort is gated
by the consuming command's policy. The gateway selects the per-command pipeline
policy where it decorates handlers.

### 4. Guard (foot-gun protection)
At pipeline-build time, if `Transactional == false` AND the command's handler is
known to enqueue to the outbox, the wiring **panics at startup** (init-time, like
the apperr duplicate-code guard) with a clear message: `Transactional:false +
outbox breaks effectively-once`. Mechanism: an explicit
`Pipeline.UsesOutbox()` marker the wiring sets for outbox commands; the guard
checks it against the policy. (If a clean static signal is impractical, fall back
to a documented WARN log + a runtime assertion in `cqrs.Transaction`'s absence.)

### 5. Demo command — `RecordProductView` (gateway)
A new low-value, high-volume command that is a **single local insert, no
outbox** (records a product view into a `product_views` table). Wired with
`Eventual.With(Transactional(false))` — no wrapping tx, async best-effort audit,
no RYW. Contrasts with `CreateOrder` (`Strong`, Kafka-choreographed). This both
demonstrates the differentiation and is the throughput-contrast subject.

## Data flow

**Strong command (CreateOrder — unchanged):**
```
inbox.ProcessOnce(tx){ handler: insert order + enqueue outbox; audit.Audit: chain-write }
  → commit  (audit failure ⇒ whole command rolls back)
```

**Eventual command (RecordProductView):**
```
handler: insert product_view (no tx wrap, no outbox)
  → respond immediately
  → audit Entry → BufferedAuditWriter.Enqueue (non-blocking; drop+metric if full)
       → background: batch by chain_id → one tx per chain → chain-write
```
The command rate is no longer bound by the audit chain; audit throughput is
itself batched (one fsync per N entries per chain).

## Guarantees preserved / relaxed

- **Always:** effectively-once delivery (outbox/inbox) for any Transactional
  command; Strong commands byte-identical; tamper-evidence (`VerifyChain` over
  the async-written chain).
- **Relaxed under Eventual (explicit, per command):** audit completeness becomes
  best-effort (may gap on overflow — alerted, never silent); read-your-writes
  (brief 404 after POST); projection lag.

## Testing

- Policy wiring: `Strong` → sync audit in-tx + Transaction present; `Eventual` →
  AsyncAudit + (with the override) no cqrs tx. Strong path regression-identical.
- `BufferedAuditWriter`: batches by chain_id (one tx per chain-group), drop +
  counter on overflow, drains on shutdown, `VerifyChain` passes over the
  asynchronously-written chain (tamper-evidence intact).
- Guard: `Transactional:false` + outbox command → startup panic.
- Effectively-once intact for a Transactional Eventual command (outbox still
  atomic; only audit/RYW relaxed).
- Throughput contrast (gated benchmark, logged not asserted): `RecordProductView`
  (Eventual) command-rate vs the same flow forced Strong — the command rate is
  decoupled from the audit chain.

## Out of scope / deferred

- Per-request runtime policy override (compile-time per-type only).
- Durable async audit (Kafka-backed) — the in-process buffered writer with
  drop-on-overflow is the chosen best-effort design; a durable variant is a later
  option.
- A2 does NOT touch the outbox/inbox delivery guarantees or the read-store choice
  (that is A1, a separate spec).
