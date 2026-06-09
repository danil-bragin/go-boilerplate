package cqrs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrDeadlineExceeded is returned by the Deadline behavior when the handler
// did not complete within its budget. It wraps context.DeadlineExceeded, so
// both errors.Is(err, cqrs.ErrDeadlineExceeded) and
// errors.Is(err, context.DeadlineExceeded) hold.
var ErrDeadlineExceeded = fmt.Errorf("cqrs: handler deadline exceeded: %w", context.DeadlineExceeded)

// Deadline returns a Behavior that bounds the handler's execution time with a
// context deadline. The handler ctx is derived via context.WithTimeout, so a
// tighter caller deadline still wins. When the handler returns an error caused
// by THIS deadline firing, the error is wrapped as ErrDeadlineExceeded;
// domain errors and caller-cancellation errors pass through untouched.
//
// Place Deadline after Metrics and before Validation in a pipeline (the
// StandardPipeline builder does this) so the duration metrics observe the
// timeout while validation and domain behaviors all run under the budget.
func Deadline[C, R any](d time.Duration) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			tctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			res, err := next(tctx, cmd)
			// Wrap only when THIS behavior's deadline fired (parent still
			// alive): a caller-side timeout/cancellation passes through as-is.
			if err != nil && ctx.Err() == nil &&
				errors.Is(tctx.Err(), context.DeadlineExceeded) &&
				errors.Is(err, context.DeadlineExceeded) {
				var zero R
				return zero, fmt.Errorf("%w (budget %s)", ErrDeadlineExceeded, d)
			}
			return res, err
		}
	}
}
