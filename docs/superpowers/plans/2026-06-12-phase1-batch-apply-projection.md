# Phase 1 — Batch-apply in the consume pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the consume pipeline a batch-apply mode so the gateway projection commits ONE DB transaction per partition-batch-per-poll instead of one per event, multiplying write throughput while preserving every guarantee (exactly-once, per-aggregate order, RYW, atomic audit).

**Architecture:** Three layers, bottom-up. (1) `inbox.ProcessBatchOnce` runs N dedup-inserts + N side-effects in ONE transaction. (2) `kafka.RunBatch` + `BatchHandlerFunc` hand a partition's poll records to a batch handler that returns how many it applied; the existing per-poll commit + seek-back machinery is refactored to be shared between the per-record `Run` and the new `RunBatch`. (3) `consume.Consumer.BatchHandler` is the batch handler: pre-tx decode/route filter → one `ProcessBatchOnce` tx → on any tx error, fall back to the existing per-record `ProcessOnce` path one record at a time, returning the applied count so kafka seeks back to the first unrecoverable record. The old per-record path is NOT removed — it is the fallback.

**Tech Stack:** Go 1.26, franz-go (`kgo`), pgx v5, testcontainers (Postgres + Redpanda), testify.

**Guarantee invariants (unbreakable — every task preserves these):**
1. Batch unit = records of ONE partition from one poll.
2. Happy path = all records' inbox-insert + side-effect in ONE tx, in offset order; `OnCommitted` hooks fire per-record after commit, in order.
3. Batch tx is strictly all-or-nothing: rollback persists NO side effect, including inbox rows.
4. Fallback (any batch-tx error) = existing per-record `ProcessOnce`, one at a time, offset order, not parallel; poison → existing tiered-retry/DLT.
5. Pre-tx decode filter: malformed → DLT immediately; only decodable records enter the tx. Runtime errors still caught by the fallback.
6. Offset commit stays per-poll, only after the whole poll's records are resolved (committed or fallback-done incl poison→DLT). Crash before → whole-poll redelivery → inbox dedup makes it safe.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `platform/messaging/inbox/inbox.go` | inbox dedup primitives | Add `ProcessBatchOnce` |
| `platform/messaging/inbox/inbox_test.go` | inbox tests | Add batch tests |
| `platform/messaging/kafka/consumer.go` | handler types | Add `BatchHandlerFunc` |
| `platform/messaging/kafka/run.go` | poll loop | Refactor to shared loop; add `RunBatch` |
| `platform/messaging/kafka/run_test.go` (or batch_test.go) | run tests | Add batch redpanda test |
| `platform/messaging/consume/batch.go` (new) | batch handler builder | Create |
| `platform/messaging/consume/batch_test.go` (new) | batch consume tests | Create |
| `platform/messaging/consume/metrics.go` (new or existing) | consume metrics | Add `batch_fallback_total` |
| `platform/servicekit/consumers.go` | consumer wiring | Add `AddBatchConsumer` |
| `examples/gateway/internal/projection/projection.go` | projection consumer | Add `NewBatchHandler` |
| `examples/gateway/gateway.go` + `cmd/projection/main.go` | wiring | Opt the projection into batch mode |
| `examples/e2e/traffic_test.go` or new | validation | Poison-injection + throughput |

---

## Task 1: `inbox.ProcessBatchOnce` — N dedup-inserts + N side-effects in one tx

**Files:**
- Modify: `platform/messaging/inbox/inbox.go`
- Test: `platform/messaging/inbox/inbox_test.go`

- [ ] **Step 1: Write the failing test**

Add to `platform/messaging/inbox/inbox_test.go` (uses the package's existing `newPool` helper + testcontainer):

```go
func TestProcessBatchOnce_AppliesAllInOneTx(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	var applied []string
	items := []inbox.BatchItem{
		{MessageID: "m1", Fn: func(ctx context.Context) error { applied = append(applied, "m1"); return nil }},
		{MessageID: "m2", Fn: func(ctx context.Context) error { applied = append(applied, "m2"); return nil }},
	}
	n, err := inbox.ProcessBatchOnce(ctx, pool, "c1", items)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []string{"m1", "m2"}, applied)

	// Re-running the same ids is a full no-op (dedup).
	applied = nil
	n, err = inbox.ProcessBatchOnce(ctx, pool, "c1", items)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, applied)
}

func TestProcessBatchOnce_RollsBackEntireBatchOnError(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	boom := errors.New("boom")
	items := []inbox.BatchItem{
		{MessageID: "g1", Fn: func(ctx context.Context) error { return nil }},
		{MessageID: "bad", Fn: func(ctx context.Context) error { return boom }},
	}
	_, err := inbox.ProcessBatchOnce(ctx, pool, "c1", items)
	require.ErrorIs(t, err, boom)

	// All-or-nothing: NO inbox row persisted, including g1's.
	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from inbox where consumer='c1'`).Scan(&count))
	require.Zero(t, count, "rollback must persist no inbox rows")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestProcessBatchOnce ./platform/messaging/inbox/ -count=1`
Expected: FAIL — `undefined: inbox.BatchItem` / `inbox.ProcessBatchOnce`.

- [ ] **Step 3: Implement `ProcessBatchOnce`**

Append to `platform/messaging/inbox/inbox.go`:

```go
// BatchItem is one message's dedup id and its side effect, for ProcessBatchOnce.
type BatchItem struct {
	MessageID string
	Fn        func(context.Context) error
}

// ProcessBatchOnce runs a slice of items in ONE transaction: for each item it
// inserts the (consumer, message_id) dedup row (ON CONFLICT DO NOTHING) and, if
// the row was newly inserted, runs Fn — all in the same tx, in slice order.
// Already-seen ids are skipped (their Fn is not called). It returns the number
// of items whose Fn ran. Any Fn error rolls back the WHOLE batch (no inbox row,
// no side effect persists), so the caller may safely fall back to per-item
// processing — the batch left nothing behind.
func ProcessBatchOnce(ctx context.Context, pool *pg.Pool, consumer string, items []BatchItem) (int, error) {
	applied := 0
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		applied = 0
		db := pg.FromContext(ctx, pool)
		for _, it := range items {
			tag, err := db.Exec(ctx,
				`insert into inbox (consumer, message_id) values ($1, $2) on conflict do nothing`,
				consumer, it.MessageID)
			if err != nil {
				return fmt.Errorf("inbox: batch insert %s: %w", it.MessageID, err)
			}
			if tag.RowsAffected() == 0 {
				continue // already processed by this consumer
			}
			if err := it.Fn(ctx); err != nil {
				return err // rolls back the entire batch, including prior rows
			}
			applied++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestProcessBatchOnce ./platform/messaging/inbox/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/messaging/inbox/inbox.go platform/messaging/inbox/inbox_test.go
git commit -m "feat(inbox): ProcessBatchOnce — N dedup-inserts + side-effects in one tx"
```

---

## Task 2: `kafka.BatchHandlerFunc` + `RunBatch` (shared poll loop)

**Files:**
- Modify: `platform/messaging/kafka/consumer.go` (add type)
- Modify: `platform/messaging/kafka/run.go` (refactor + `RunBatch`)
- Test: `platform/messaging/kafka/batch_test.go` (new)

**Refactor note:** `run.go`'s `Run` poll loop (lines ~76–275) stays as-is EXCEPT the per-partition goroutine body (the `part.EachRecord` block, ~187–203) is extracted into a processor parameter. Both `Run` and `RunBatch` call a shared unexported `run(ctx, processPartition)`.

- [ ] **Step 1: Add the batch handler type**

In `platform/messaging/kafka/consumer.go`, next to `HandlerFunc`:

```go
// BatchHandlerFunc processes a slice of records from ONE partition (one poll).
// It returns the number of records it durably applied, in order: the consumer
// commits up to records[processed-1] and, when processed < len(records) with a
// non-nil error, seeks back to records[processed] for redelivery (same
// at-least-once, per-partition semantics as HandlerFunc). processed == len means
// the whole batch applied.
type BatchHandlerFunc func(ctx context.Context, records []Record) (processed int, err error)
```

- [ ] **Step 2: Write the failing test**

Create `platform/messaging/kafka/batch_test.go` (mirror the redpanda harness used by `run_test.go` — reuse `kafkatest.NewRedpanda` / the package's existing producer+consumer test helpers):

```go
func TestRunBatch_DeliversPartitionRecordsAndCommits(t *testing.T) {
	// produce 5 records to one partition, consume with RunBatch, assert the
	// handler saw them as a single ordered slice and offsets advanced.
	// (Use the same setup pattern as run_test.go's happy-path test.)
	...
	var got [][]string
	bh := func(ctx context.Context, recs []kafka.Record) (int, error) {
		batch := make([]string, len(recs))
		for i, r := range recs { batch[i] = string(r.Value) }
		got = append(got, batch)
		return len(recs), nil
	}
	// run RunBatch in a goroutine until 5 records seen, then cancel.
	...
	require.Equal(t, []string{"0","1","2","3","4"}, flatten(got))
}

func TestRunBatch_PartialFailureSeeksBack(t *testing.T) {
	// handler returns processed=2,err on first call (records 0..4): records
	// 0,1 commit; redelivery restarts at record 2. Second call sees 2,3,4 and
	// returns processed=3. Assert no record is lost and 2 is redelivered.
	...
}
```

(Fill the harness from `run_test.go`; the assertions above are the contract.)

- [ ] **Step 3: Run test to verify it fails**

Run: `go test -run TestRunBatch ./platform/messaging/kafka/ -count=1 -p 1`
Expected: FAIL — `undefined: (*Consumer).RunBatch`.

- [ ] **Step 4: Refactor `Run` + implement `RunBatch`**

In `run.go`, define the processor type and extract the shared loop. The shared `run` is the CURRENT `Run` body with the `part.EachRecord` block (lines ~182–203) replaced by a call to `processPartition(ctx, part.Records)` returning `(last, firstBad *kgo.Record)`:

```go
// partitionProcessor processes one partition's poll records in order and
// returns the last durably-handled record (for the commit) and the first
// unrecoverable record (for the seek-back), or nil firstBad on full success.
type partitionProcessor func(ctx context.Context, recs []*kgo.Record) (last, firstBad *kgo.Record)

// Run processes one record at a time (unchanged behaviour).
func (c *Consumer) Run(ctx context.Context, h HandlerFunc) error {
	return c.run(ctx, func(ctx context.Context, recs []*kgo.Record) (last, firstBad *kgo.Record) {
		var succeeded int64
		for _, rec := range recs {
			start := time.Now()
			err := h(ctx, RecordFromKGO(rec))
			c.metrics.recordHandlerDuration(ctx, rec.Topic, time.Since(start), err)
			if err != nil {
				firstBad = rec
				break
			}
			succeeded++
			last = rec
		}
		c.metrics.addProcessed(ctx, topicOf(recs), succeeded)
		if firstBad != nil {
			c.metrics.addFailed(ctx, firstBad.Topic)
		}
		return last, firstBad
	})
}

// RunBatch hands each partition's poll records to bh as one slice.
func (c *Consumer) RunBatch(ctx context.Context, bh BatchHandlerFunc) error {
	return c.run(ctx, func(ctx context.Context, recs []*kgo.Record) (last, firstBad *kgo.Record) {
		if len(recs) == 0 {
			return nil, nil
		}
		records := make([]Record, len(recs))
		for i, rec := range recs {
			records[i] = RecordFromKGO(rec)
		}
		start := time.Now()
		processed, err := bh(ctx, records)
		c.metrics.recordHandlerDuration(ctx, recs[0].Topic, time.Since(start), err)
		if processed > 0 {
			last = recs[processed-1]
			c.metrics.addProcessed(ctx, recs[0].Topic, int64(processed))
		}
		if err != nil && processed < len(recs) {
			firstBad = recs[processed]
			c.metrics.addFailed(ctx, recs[0].Topic)
		}
		return last, firstBad
	})
}
```

Then rename the current `Run` body to `func (c *Consumer) run(ctx context.Context, processPartition partitionProcessor) error`, and inside the partition goroutine replace the `part.EachRecord` block with:

```go
last, firstBad := processPartition(ctx, part.Records)
mu.Lock()
defer mu.Unlock()
if last != nil {
	lastGood = append(lastGood, last)
}
if firstBad != nil {
	if failed == nil {
		failed = map[tp]*kgo.Record{}
	}
	failed[tp{firstBad.Topic, firstBad.Partition}] = firstBad
}
```

Add the tiny helper:

```go
func topicOf(recs []*kgo.Record) string {
	if len(recs) == 0 {
		return ""
	}
	return recs[0].Topic
}
```

The lag-recording block (~172–175), `wg`, `mu`, commit, backoff/seek, and `AllowRebalance` all stay exactly as they are.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./platform/messaging/kafka/ -count=1 -p 1`
Expected: PASS — the new batch tests AND every existing `run_test.go` test (the per-record path is unchanged behaviourally).

- [ ] **Step 6: Commit**

```bash
git add platform/messaging/kafka/consumer.go platform/messaging/kafka/run.go platform/messaging/kafka/batch_test.go
git commit -m "feat(kafka): RunBatch + shared poll loop (per-record Run unchanged)"
```

---

## Task 3: `consume.Consumer.BatchHandler` — pre-tx filter → batch tx → per-record fallback

**Files:**
- Create: `platform/messaging/consume/batch.go`
- Test: `platform/messaging/consume/batch_test.go`

This reuses the existing per-record machinery in `consume.go`: the same ctx-install (principal/tenant/chain-lineage), the same `byType` event-type routing, the same `msgID` policy, and the same `inbox.ProcessOnce` path as the fallback. Factor the per-record "decode + route + ctx-prep" into a helper both paths call so nothing is duplicated.

- [ ] **Step 1: Extract the shared per-record prep in `consume.go`**

In `consume.go`, refactor the closure returned by `Handler` so its body (ctx install, event-type lookup, msgID, chain-lineage) lives in an unexported method:

```go
// prepared holds everything resolved from a record before the tx: the
// lineage-enriched ctx, the routed handler, the message id, and whether the
// event type was unknown (skip) — so both the per-record and batch paths share
// one decode/route implementation.
type prepared struct {
	ctx     context.Context
	h       handler // the typed handler (nil when skip)
	msgID   string
	skip    bool
}

func (c *Consumer) prepare(ctx context.Context, r kafka.Record) prepared {
	ctx = auth.ExtractToContext(ctx, r.Headers)
	ctx = tenant.ExtractToContext(ctx, r.Headers)
	eventType := r.Headers[kafka.HeaderEventType]
	h, ok := c.byType[eventType]
	if !ok {
		c.logger.DebugContext(ctx, "consume: skipping unknown event type",
			"event_type", eventType, "topic", r.Topic, "group", c.group)
		return prepared{ctx: ctx, skip: true}
	}
	msgID := r.Headers[kafka.HeaderMessageID]
	if msgID == "" {
		msgID = fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset)
	}
	corrID := r.Headers[msgctx.HeaderCorrelationID]
	if corrID == "" {
		corrID = msgID
	}
	ctx = msgctx.WithCorrelationID(ctx, corrID)
	ctx = msgctx.WithParentMessageID(ctx, msgID)
	return prepared{ctx: ctx, h: h, msgID: msgID}
}
```

Store `byType` on the `Consumer` (move it out of the `Handler` local) so both paths see it. Rewrite the existing `Handler` per-record closure to call `c.prepare(ctx, r)` then its existing `inbox.ProcessOnce` block. Run the existing consume tests to prove no behaviour change:

Run: `go test ./platform/messaging/consume/ -count=1 -p 1` → PASS.

- [ ] **Step 2: Write the failing batch test**

Create `platform/messaging/consume/batch_test.go`. Use the package's existing test harness (real pg + a typed handler + `fakes.Broker` or the existing consume test setup). Assertions:

```go
// Happy path: a clean batch applies all side-effects in ONE tx (assert via a
// handler that records ctx tx identity, or via a single commit observed).
func TestBatchHandler_AppliesCleanBatchInOneTx(t *testing.T) { ... }

// Poison fallback: batch of [good, poison, good2]; batch tx fails on poison,
// fallback applies good, then poison returns processed=1,err so the kafka layer
// seeks back to poison. Assert: good applied exactly once, good2 NOT applied
// (it is after the poison in offset order), processed==1, err!=nil.
func TestBatchHandler_PoisonFallsBackAndReportsProcessed(t *testing.T) { ... }

// Dedup: re-delivering an already-seen batch applies nothing (processed counts
// only newly-applied), and the OnCommitted hook does not re-fire for seen ids.
func TestBatchHandler_DedupSkipsSeen(t *testing.T) { ... }
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test -run TestBatchHandler ./platform/messaging/consume/ -count=1 -p 1`
Expected: FAIL — `undefined: (*Consumer).BatchHandler`.

- [ ] **Step 4: Implement `BatchHandler`**

Create `platform/messaging/consume/batch.go`:

```go
package consume

import (
	"context"
	"fmt"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
)

// BatchHandler builds a kafka.BatchHandlerFunc over the typed handlers. The
// happy path applies every decodable record's side-effect + inbox dedup in ONE
// transaction (inbox.ProcessBatchOnce). On any batch-tx error it falls back to
// the per-record path (the same inbox.ProcessOnce the non-batch Handler uses),
// one record at a time in offset order, returning the count it applied so the
// kafka layer seeks back to the first unrecoverable record. OnCommitted hooks
// fire per-record, in order, after the owning commit. Pre-tx, unknown event
// types are skipped (acked) and decode failures route to the per-record path so
// the existing permanent-error → DLT handling applies unchanged.
func (c *Consumer) BatchHandler(handlers ...Handler) kafka.BatchHandlerFunc {
	c.index(handlers...) // populates c.byType (shared with Handler)

	return func(ctx context.Context, records []kafka.Record) (int, error) {
		// Pre-tx: resolve each record. Unknown types are acked-skipped; the rest
		// become batch items carrying the per-record decode (errors surface in
		// the handler closure inside the tx and trigger the fallback).
		type rec struct {
			r           kafka.Record
			p           prepared
			onCommitted func(context.Context)
		}
		live := make([]rec, 0, len(records))
		for _, r := range records {
			p := c.prepare(ctx, r)
			if p.skip {
				continue
			}
			live = append(live, rec{r: r, p: p})
		}
		if len(live) == 0 {
			return len(records), nil // all skipped — fully acked
		}

		// Happy path: one tx for the whole batch.
		items := make([]inbox.BatchItem, len(live))
		for i := range live {
			i := i
			items[i] = inbox.BatchItem{
				MessageID: live[i].p.msgID,
				Fn: func(ctx context.Context) error {
					oc, err := live[i].p.h.handle(ctx, c.dec, live[i].r.Value)
					if err != nil {
						return err
					}
					live[i].onCommitted = oc
					return nil
				},
			}
		}
		if _, err := inbox.ProcessBatchOnce(ctx, c.pool, c.group, items); err == nil {
			// Committed: fire post-commit hooks in order, then ack the whole poll.
			for i := range live {
				if live[i].onCommitted != nil {
					live[i].onCommitted(live[i].p.ctx)
				}
			}
			return len(records), nil
		}

		// Fallback: per-record, in offset order, reusing the proven path. Return
		// the count applied before the first unrecoverable record so kafka seeks
		// back to it. NB: index into `records` (not `live`) so the seek offset is
		// correct even when earlier records were skipped.
		c.metrics.addBatchFallback(ctx)
		applied := 0
		for ri, r := range records {
			if err := c.processRecord(ctx, r); err != nil {
				return ri, err // seek back to records[ri]
			}
			applied = ri + 1
		}
		return applied, nil
	}
}
```

Add `c.processRecord(ctx, r)` to `consume.go` by extracting the existing per-record `inbox.ProcessOnce` block from `Handler`'s closure (so `Handler` and the fallback share ONE implementation):

```go
func (c *Consumer) processRecord(ctx context.Context, r kafka.Record) error {
	p := c.prepare(ctx, r)
	if p.skip {
		return nil
	}
	if c.noInbox {
		oc, err := p.h.handle(p.ctx, c.dec, r.Value)
		if err != nil {
			return fmt.Errorf("consume: process %s: %w", p.msgID, err)
		}
		if oc != nil {
			oc(p.ctx)
		}
		return nil
	}
	var oc func(context.Context)
	_, err := inbox.ProcessOnce(p.ctx, c.pool, c.group, p.msgID, func(ctx context.Context) error {
		var herr error
		oc, herr = p.h.handle(ctx, c.dec, r.Value)
		return herr
	})
	if err != nil {
		return fmt.Errorf("consume: process %s: %w", p.msgID, err)
	}
	if oc != nil {
		oc(p.ctx)
	}
	return nil
}
```

Make the existing `Handler` return `func(ctx, r) error { return c.processRecord(ctx, r) }`, and `index`/`byType` shared. (`index` is the small helper that fills `c.byType` and panics on duplicate event type — moved out of `Handler`.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./platform/messaging/consume/ -count=1 -p 1`
Expected: PASS — new batch tests + all existing consume tests (the per-record path now routes through `processRecord` but is behaviourally identical).

- [ ] **Step 6: Commit**

```bash
git add platform/messaging/consume/batch.go platform/messaging/consume/consume.go platform/messaging/consume/batch_test.go
git commit -m "feat(consume): BatchHandler — optimistic batch tx + per-record fallback"
```

---

## Task 4: `batch_fallback_total` metric

**Files:**
- Modify: `platform/messaging/consume/metrics.go` (create if absent; otherwise extend the existing consume metrics struct)

- [ ] **Step 1: Add the counter**

In the consume metrics constructor (global meter `messaging.consume`, nil-degrading like `outbox`/`relayMetrics`):

```go
func (m consumeMetrics) addBatchFallback(ctx context.Context) {
	if m.batchFallback != nil {
		m.batchFallback.Add(ctx, 1)
	}
}
```

with the instrument:

```go
if c, err := meter.Int64Counter("consume.batch_fallback_total",
	metric.WithDescription("Batch-apply transactions that fell back to per-record processing (poison or runtime error)")); err == nil {
	m.batchFallback = c
}
```

Wire `c.metrics` on the `Consumer` if it does not already carry one.

- [ ] **Step 2: Build + run consume tests**

Run: `go build ./platform/messaging/consume/... && go test ./platform/messaging/consume/ -count=1 -p 1`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add platform/messaging/consume/
git commit -m "feat(consume): batch_fallback_total metric (poison/fallback rate)"
```

---

## Task 5: `servicekit.AddBatchConsumer`

**Files:**
- Modify: `platform/servicekit/consumers.go`
- Test: extend `platform/servicekit/consumers_test.go` if present (else rely on the e2e in Task 7)

- [ ] **Step 1: Implement (mirror `AddConsumer`, calling `RunBatch`)**

Find `AddConsumer` in `consumers.go` and add a sibling that takes a `kafka.BatchHandlerFunc` and launches `consumer.RunBatch` instead of `consumer.Run`. Everything else (EnsureTopics, group, lifecycle goroutine, OnError) is identical — extract any shared setup so this is DRY:

```go
// AddBatchConsumer is AddConsumer for a batch handler: each partition's poll
// records are applied in one transaction (consume.Consumer.BatchHandler). Use it
// for high-volume idempotent projections where per-event commits are the
// bottleneck. Same lifecycle, topics, and error handling as AddConsumer.
func (s *Service) AddBatchConsumer(ctx context.Context, name string, topics []string, h kafka.BatchHandlerFunc, opts ...kafka.ConsumerOption) error {
	// ... identical to AddConsumer except the goroutine calls consumer.RunBatch(ctx, h)
}
```

- [ ] **Step 2: Build**

Run: `go build ./platform/servicekit/...`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add platform/servicekit/consumers.go
git commit -m "feat(servicekit): AddBatchConsumer wiring"
```

---

## Task 6: Opt the gateway projection into batch mode

**Files:**
- Modify: `examples/gateway/internal/projection/projection.go` (add `NewBatchHandler`)
- Modify: `examples/gateway/gateway.go` and `examples/gateway/cmd/projection/main.go` (wire it behind `GATEWAY_PROJECTION_BATCH`, default true for the projection)
- Modify: `.env.example`, `docs/operations.md`

- [ ] **Step 1: Add `NewBatchHandler`**

In `projection.go`, add a sibling to `NewHandlerWithStore` that returns a `kafka.BatchHandlerFunc` by calling `consume.New(...).BatchHandler(...)` with the SAME `consume.TypedFor(...)` handlers already defined — extract the handler list so `NewHandler` and `NewBatchHandler` share it (DRY, no duplicated projection logic):

```go
func NewBatchHandler(pool *pg.Pool, logger *slog.Logger, cache cqrs.Cache, notify StatusNotifier, opts ...consume.Option) kafka.BatchHandlerFunc {
	storeFor := func(ctx context.Context) Store { return storegen.New(pg.FromContext(ctx, pool)) }
	opts = append([]consume.Option{consume.WithLogger(logger)}, opts...)
	return consume.New(pool, consumerGroup, opts...).BatchHandler(projectionHandlers(pool, storeFor, logger, cache, notify)...)
}
```

where `projectionHandlers(...) []consume.Handler` is the extracted `consume.TypedFor(...)` slice currently inline in `NewHandlerWithStore`.

- [ ] **Step 2: Wire behind the env flag**

Where the gateway/`cmd/projection` currently calls `svc.AddConsumer(... projection.NewHandler(...) ...)`, branch on a new `GATEWAY_PROJECTION_BATCH` config (default `true`): true → `svc.AddBatchConsumer(... projection.NewBatchHandler(...) ...)`; false → the existing per-event path.

- [ ] **Step 3: Build + run gateway tests**

Run: `go test -p 1 ./examples/gateway/... -count=1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add examples/gateway/ .env.example docs/operations.md
git commit -m "feat(gateway): projection batch-apply mode (GATEWAY_PROJECTION_BATCH)"
```

---

## Task 7: Validation — poison injection + throughput, no regressions

**Files:**
- Modify/add: `examples/e2e/traffic_test.go` (or a new `examples/e2e/batch_projection_test.go`)

- [ ] **Step 1: Correctness with poison injection**

Add a test that drives the full stack (the `TestE2E_Traffic` harness) with the projection in batch mode AND a small fraction of malformed/poison events injected into `orders.events`/`payments.events`. Assert via the correctness ledger: every clean order still reaches its terminal status; each poison event lands in the DLT; per-key order and exactly-once hold (one orders row per id, one terminal status). Run twice for stability.

Run: `go test -p 1 -run TestE2E_Traffic ./examples/e2e/ -count=1`
Expected: PASS, zero ledger violations.

- [ ] **Step 2: Throughput before/after (recorded, not asserted in CI)**

Add a benchmark-style measurement (gated behind an env flag like the existing `TRAFFIC_ASSERT_LATENCY`) that records committed projection tx/s with `GATEWAY_PROJECTION_BATCH=false` vs `true` at a fixed input rate, and logs both. Capture the numbers for the commit message — this is the "before/after" the spec requires.

- [ ] **Step 3: Crash-safety assertion**

Add/extend a test proving a crash mid-poll (cancel the consumer ctx before the offset commit) leaves the offset un-advanced and produces no duplicate projection rows after restart (inbox dedup). Reuse the existing inbox-dedup e2e pattern.

- [ ] **Step 4: Commit**

```bash
git add examples/e2e/
git commit -m "test(e2e): batch projection — poison isolation, throughput, crash-safety"
```

---

## Re-measure gate (per the spec)

After Task 7, run `testkit/traffic` at the 5k order-create target and identify the first saturating writer. If none saturates, Phase 1 + DB-per-service spread already closes 5k and **Phase 2 (`ShardedPool`) becomes a headroom/linearity proof, not a necessity** — record the per-writer ceiling and decide by the measurement, not assumption. Each subsequent phase is its own plan.

## Self-review notes

- Guarantee coverage: invariants 1–6 map to Task 1 (all-or-nothing tx), Task 2 (per-poll commit + seek-back via `processed`), Task 3 (pre-tx routing filter, fallback, offset order, OnCommitted ordering), Task 7 (poison + crash + exactly-once proofs). RYW untouched (REST pending insert not in scope).
- No duplication: per-record decode/route extracted to `c.prepare`/`c.processRecord`; projection handler list extracted to `projectionHandlers`; `AddBatchConsumer` shares `AddConsumer` setup; `Run`/`RunBatch` share the `run` poll loop.
- Old per-record path retained as the fallback (invariant 4).

### OPEN ITEM — invariant 5 (pre-tx decode), decide before Task 3

The plan's `prepare` does header ROUTING pre-tx (event-type, msgID, lineage) but
the PAYLOAD decode happens inside the typed handler (`h.handle`), i.e. inside the
batch tx. So a malformed payload triggers one batch-tx rollback → fallback →
per-record → DLT. The **guarantee is preserved** (malformed reaches the DLT,
never double-applied), but this is "fallback handles malformed", not invariant
5's literal "decode pre-tx, malformed → DLT immediately, never enters the tx".

Two options — pick one at execution time:
- **(A) Accept as-is (recommended):** the fallback already routes malformed to
  the DLT correctly; the only cost is one wasted batch-tx rollback when a poison
  record is in the batch (rare — tracked by `batch_fallback_total`). Zero new
  surface.
- **(B) True pre-tx decode:** split the typed handler into `decode(dec, value)
  (decoded, error)` + `apply(ctx, decoded) (onCommitted, error)` (a change to
  `consume.TypedFor`), decode all records in `prepare`, route decode failures
  straight to the per-record/DLT path, and pass only decoded payloads into the
  batch tx. Honors invariant 5 literally; adds a handler-abstraction change.

This is the one place the plan does not match the spec's letter; flagged for an
explicit decision rather than silently chosen.
