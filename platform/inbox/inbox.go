// Package inbox implements the idempotent-consumer (inbox) pattern: a message
// is processed at most once per consumer. ProcessOnce records the message id
// and runs the side effect in the SAME transaction, so dedup and effect commit
// atomically — giving effectively-once processing over at-least-once delivery.
package inbox

import (
	"context"
	"fmt"

	"go-boilerplate/platform/pg"
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
