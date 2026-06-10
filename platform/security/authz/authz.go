// Package authz provides role-based and resource-aware authorization as a
// CQRS behavior.
package authz

import (
	"context"
	"fmt"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"
)

// Both sentinels are apperr errors (AUTH_UNAUTHENTICATED / AUTH_FORBIDDEN),
// so httpx.FromError maps them to 401/403 with stable codes automatically.
// errors.Is against the sentinels keeps working: apperr matches by code,
// including through fmt.Errorf("%w", …) wrapping.
var (
	// ErrUnauthenticated is returned when no principal is present in the context.
	ErrUnauthenticated error = apperr.New(apperr.CodeAuthUnauthenticated)
	// ErrForbidden is returned when the principal lacks the required role.
	ErrForbidden error = apperr.New(apperr.CodeAuthForbidden)
)

// Policy decides whether a principal may perform an action on a resource.
//
// resource is the domain object being acted upon (e.g. an order view) and MAY
// be nil for pure role-gated actions. Role-only policies (RBAC) ignore it;
// resource-aware policies (ownership checks, ABAC) inspect it. ctx carries
// request-scoped data (deadline, trace) — policies must not block on it
// indefinitely.
type Policy interface {
	Authorize(ctx context.Context, p auth.Principal, action string, resource any) error
}

// RBAC maps actions to the set of roles allowed to perform them. A principal
// is authorized if it holds ANY of the action's required roles. An action with
// no entry is denied by default (deny-by-default). RBAC is purely role-based:
// the resource argument is ignored.
type RBAC struct {
	rules map[string][]string // action -> allowed roles
}

// NewRBAC constructs an RBAC policy from an action→roles map.
func NewRBAC(rules map[string][]string) *RBAC { return &RBAC{rules: rules} }

// Authorize returns nil if p holds any role permitted for action, or ErrForbidden.
// The resource argument is ignored (RBAC is role-only).
func (r *RBAC) Authorize(_ context.Context, p auth.Principal, action string, _ any) error {
	allowed, ok := r.rules[action]
	if !ok {
		return fmt.Errorf("%w: action %q not permitted", ErrForbidden, action)
	}
	for _, role := range p.Roles {
		for _, a := range allowed {
			if role == a {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: principal lacks role for %q", ErrForbidden, action)
}

// Require returns a CQRS behavior that authorizes the ctx principal for the
// given action before invoking the handler. No principal in context →
// ErrUnauthenticated; policy denial → ErrForbidden; otherwise the handler runs.
//
// The command itself is passed as the resource, so resource-aware policies can
// inspect it. Role-only policies (RBAC) ignore it.
func Require[C, R any](policy Policy, action string) cqrs.Behavior[C, R] {
	return func(next cqrs.HandlerFunc[C, R]) cqrs.HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			var zero R
			p, ok := auth.From(ctx)
			if !ok {
				return zero, ErrUnauthenticated
			}
			if err := policy.Authorize(ctx, p, action, cmd); err != nil {
				return zero, err
			}
			return next(ctx, cmd)
		}
	}
}
