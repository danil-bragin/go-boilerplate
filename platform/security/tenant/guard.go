package tenant

import (
	"context"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
)

// ErrRequired is returned by the Require behavior when no tenant is present in
// the context. It is an apperr (TENANT_REQUIRED, 400, permanent), so
// httpx.FromError maps it to 400 with a stable code, and the async retry path
// short-circuits it straight to the DLT (permanent → no redelivery). errors.Is
// against the sentinel works through fmt.Errorf("%w", …) wrapping.
var ErrRequired error = apperr.New(apperr.CodeTenantRequired)

// Require returns a CQRS behavior that fails closed with ErrRequired when the
// context carries no tenant, before invoking the handler. Wrap tenant-scoped
// command handlers with it so a request that slipped past tenant resolution can
// never mutate state under an unknown tenant.
//
// Place it INSIDE authentication/authorization but OUTSIDE the transaction, so
// a missing tenant is rejected before any DB work begins.
func Require[C, R any]() cqrs.Behavior[C, R] {
	return func(next cqrs.HandlerFunc[C, R]) cqrs.HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			if _, ok := FromContext(ctx); !ok {
				var zero R
				return zero, ErrRequired
			}
			return next(ctx, cmd)
		}
	}
}
