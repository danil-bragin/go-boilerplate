package consume

import (
	"context"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
)

// BatchHandler combines the given typed handlers into a kafka.BatchHandlerFunc
// that applies a partition's poll records optimistically in ONE transaction,
// falling back to the proven per-record path on any batch-tx error.
//
// Happy path: every decodable record's inbox-insert + side effect runs inside a
// single inbox.ProcessBatchOnce transaction, in offset order; on commit, each
// record's OnCommitted hook fires per-record, in order, AFTER the commit. Unknown
// event types are skipped (acked) before the transaction — they never enter the
// batch and never count as failures (forward compatibility, same as Handler).
//
// Fallback: ProcessBatchOnce is strictly all-or-nothing (a failing record rolls
// the whole batch back, persisting nothing), so on ANY batch-tx error the handler
// replays the ORIGINAL records one at a time through processRecord — the same
// inbox.ProcessOnce path Handler uses — in offset order, not in parallel. It
// returns processed = the count applied before the first record whose per-record
// processing errored, plus that error, so the kafka layer commits up to
// records[processed-1] and seeks back to records[processed] for redelivery.
// Because the batch left nothing behind, the fallback re-applies each record at
// most once (no double-apply). Decode of a malformed payload happens inside the
// typed handler (inside the tx), so it triggers rollback → fallback → per-record
// → existing DLT handling.
func (c *Consumer) BatchHandler(handlers ...Handler) kafka.BatchHandlerFunc {
	c.index(handlers...)

	return func(ctx context.Context, records []kafka.Record) (int, error) {
		// rec pairs a record with its prepared per-record state and a slot for
		// the post-commit hook the side effect produces.
		type rec struct {
			r           kafka.Record
			p           prepared
			onCommitted func(context.Context)
		}

		// Pre-tx route: install ctx + dispatch, dropping unknown event types
		// (acked-skipped — they don't enter the batch and aren't failures).
		live := make([]rec, 0, len(records))
		for _, r := range records {
			p := c.prepare(ctx, r)
			if p.skip {
				continue
			}
			live = append(live, rec{r: r, p: p})
		}
		if len(live) == 0 {
			return len(records), nil
		}

		// Build one batch item per live record. The Fn runs inside the shared
		// tx; it stores the post-commit hook back into the slice element so the
		// happy path can fire hooks in order after the commit. i := i shadows
		// the loop var so each closure captures its own index.
		// Go 1.22+ gives each iteration its own i, so the closures below
		// capture distinct indices without an explicit shadow.
		items := make([]inbox.BatchItem, len(live))
		for i := range live {
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
			// Happy path: fire post-commit hooks per-record, in offset order,
			// after the commit. Only records whose Fn actually ran (not deduped)
			// have a non-nil hook stored.
			for i := range live {
				if live[i].onCommitted != nil {
					live[i].onCommitted(live[i].p.ctx)
				}
			}
			return len(records), nil
		}

		// Fallback: replay the ORIGINAL records per-record, in offset order,
		// reusing the proven processRecord path. Index into records so the
		// returned offset matches what kafka must seek back to.
		// NOTE: the addBatchFallback metric is intentionally omitted here —
		// the consume.Consumer has no metrics field yet (Task 4 adds it).
		applied := 0
		for ri, r := range records {
			if err := c.processRecord(ctx, r); err != nil {
				return ri, err
			}
			applied = ri + 1
		}
		return applied, nil
	}
}
