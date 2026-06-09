package cqrs

import (
	"context"

	"go-boilerplate/platform/storage/pg"
)

// Transaction wraps a command handler so it executes inside a database
// transaction (pg.RunInTx). The handler's repository calls — via
// pg.FromContext — observe the tx, so all writes (including outbox enqueue)
// commit atomically, or roll back together on error. Apply this ONLY to
// commands; queries must remain read-only and must NOT use this behavior.
func Transaction[C, R any](pool *pg.Pool) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			var res R
			err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
				var err error
				res, err = next(ctx, cmd)
				return err
			})
			if err != nil {
				var zero R
				return zero, err
			}
			return res, nil
		}
	}
}
