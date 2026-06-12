// Package inbox implements the idempotent-consumer (inbox) pattern: a message
// is processed at most once per consumer. ProcessOnce records the message id
// and runs the side effect in the SAME transaction, so dedup and effect commit
// atomically — giving effectively-once processing over at-least-once delivery.
package inbox

import (
	"context"
	"fmt"

	"go-boilerplate/platform/storage/pg"
)

// ProcessOnce runs fn exactly once per (consumer, messageID) pair.
//
// It opens a transaction, attempts to insert a dedup row into the inbox table
// using ON CONFLICT DO NOTHING, and checks the number of rows affected:
//
//   - If 0 rows were inserted the message was already processed by this
//     consumer: fn is NOT called and (false, nil) is returned.
//   - If 1 row was inserted fn is called within the same transaction so the
//     side effect and the inbox marker commit atomically.  On success
//     (true, nil) is returned.  If fn returns an error the transaction is
//     rolled back — the inbox row is NOT persisted — and the error is returned
//     so the message can be reprocessed later.
func ProcessOnce(ctx context.Context, pool *pg.Pool, consumer, messageID string, fn func(context.Context) error) (bool, error) {
	var processed bool
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		tag, err := pg.FromContext(ctx, pool).Exec(ctx,
			`insert into inbox (consumer, message_id) values ($1, $2) on conflict do nothing`,
			consumer, messageID)
		if err != nil {
			return fmt.Errorf("inbox: insert: %w", err)
		}
		if tag.RowsAffected() == 0 {
			processed = false // already processed by this consumer
			return nil
		}
		if err := fn(ctx); err != nil {
			return err // rolls back the inbox row too
		}
		processed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return processed, nil
}

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
