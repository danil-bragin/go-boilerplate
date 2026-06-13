package consume

import (
	"context"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"
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
// replays the ORIGINAL records one at a time through the fallback handler — in
// offset order, not in parallel. It returns processed = the count applied before
// the first record whose per-record processing errored, plus that error, so the
// kafka layer commits up to records[processed-1] and seeks back to
// records[processed] for redelivery. Because the batch left nothing behind, the
// fallback re-applies each record at most once (no double-apply). Decode of a
// malformed payload happens inside the typed handler (inside the tx), so it
// triggers rollback → fallback → per-record → existing DLT handling.
//
// fallback is the per-record handler the batch falls back to on a batch-tx error.
// When nil it defaults to c.processRecord (the proven inbox.ProcessOnce path
// Handler uses). servicekit.AddBatchConsumer passes the kafka.WithRetry-wrapped
// fallback so poison records replayed in the fallback escalate to <topic>.DLT
// (failure-contract parity with AddConsumer) instead of hot-looping the partition.
func (c *Consumer) BatchHandler(fallback kafka.HandlerFunc, handlers ...Handler) kafka.BatchHandlerFunc {
	c.index(handlers...)

	if fallback == nil {
		fallback = c.processRecord
	}

	return func(ctx context.Context, records []kafka.Record) (int, error) {
		// rec pairs a record with its prepared per-record state and a slot for
		// the post-commit hook the side effect produces.
		type rec struct {
			r           kafka.Record
			p           prepared
			onCommitted func(context.Context)
		}

		// Pre-tx route: install ctx + dispatch, dropping unknown event types
		// (acked-skipped — they don't enter the batch and aren't failures). The
		// shard key (Kafka record key = aggregate id) is installed on each
		// record's ctx so the handler's repos resolve the same shard as that
		// record's inbox tx, and post-commit hooks below route correctly too.
		live := make([]rec, 0, len(records))
		for _, r := range records {
			p := c.prepare(ctx, r)
			if p.skip {
				continue
			}
			p.ctx = pg.WithShardKey(p.ctx, string(r.Key))
			live = append(live, rec{r: r, p: p})
		}
		if len(live) == 0 {
			return len(records), nil
		}

		// SHARD GROUPING: a poll batch may span shards (M>1), but inbox.ProcessBatchOnce
		// runs ONE tx on ONE pool, so we group the live records by their resolved
		// shard and run one ProcessBatchOnce per shard-group on that shard's pool.
		// At M=1 every key resolves to the one pool, so there is exactly one group
		// == today's single batch tx (M=1 behaviour identical). Within a group the
		// records keep their original offset order (range over live is in order, and
		// we append per group preserving it), so per-record OnCommitted ordering and
		// the all-or-nothing semantics are unchanged per shard. On ANY shard-group
		// error we fall back to the proven per-record replay over the ORIGINAL
		// records (which re-routes each record to its own shard via processRecord);
		// because every group is all-or-nothing it leaves nothing behind, so the
		// fallback re-applies each record at most once.
		type shardGroup struct {
			pool *pg.Pool
			idx  []int // indices into live, in offset order
		}
		groups := make([]*shardGroup, 0, 1)
		byPool := make(map[*pg.Pool]*shardGroup, 1)
		for i := range live {
			sp := c.pool.Resolve(string(live[i].r.Key))
			g := byPool[sp]
			if g == nil {
				g = &shardGroup{pool: sp}
				byPool[sp] = g
				groups = append(groups, g)
			}
			g.idx = append(g.idx, i)
		}

		// Build one batch item per live record, keyed by index, so each shard
		// group can assemble its own item slice. The Fn runs inside that shard's
		// tx; it stores the post-commit hook back into the slice element so the
		// happy path can fire hooks in order after the commit. Go 1.22+ gives each
		// iteration its own i, so the closures capture distinct indices.
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

		// Run each shard-group's batch tx on its own pool, with the group's shard
		// key on the tx ctx so the handlers' repos route to that shard.
		allOK := true
		for _, g := range groups {
			gItems := make([]inbox.BatchItem, len(g.idx))
			for j, i := range g.idx {
				gItems[j] = items[i]
			}
			// Every record in a group shares one shard; use the first record's key.
			gctx := pg.WithShardKey(ctx, string(live[g.idx[0]].r.Key))
			if _, err := inbox.ProcessBatchOnce(gctx, g.pool, c.group, gItems); err != nil {
				allOK = false
				break
			}
		}

		if allOK {
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
		// through the fallback handler. Index into records so the returned
		// offset matches what kafka must seek back to.
		c.metrics.addBatchFallback(ctx)
		applied := 0
		for ri, r := range records {
			if err := fallback(ctx, r); err != nil {
				return ri, err
			}
			applied = ri + 1
		}
		return applied, nil
	}
}
