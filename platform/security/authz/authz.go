// Package authz provides role-based authorization as a CQRS behavior.
package authz

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"
)

var (
	// ErrUnauthenticated is returned when no principal is present in the context.
	ErrUnauthenticated = errors.New("authz: no authenticated principal")
	// ErrForbidden is returned when the principal lacks the required role.
	ErrForbidden = errors.New("authz: forbidden")
)

// Policy decides whether a principal may perform an action.
type Policy interface {
	Authorize(p auth.Principal, action string) error
}

// RBAC maps actions to the set of roles allowed to perform them. A principal
// is authorized if it holds ANY of the action's required roles. An action with
// no entry is denied by default (deny-by-default).
type RBAC struct {
	rules map[string][]string // action -> allowed roles
}

// NewRBAC constructs an RBAC policy from an action→roles map.
func NewRBAC(rules map[string][]string) *RBAC { return &RBAC{rules: rules} }

// Authorize returns nil if p holds any role permitted for action, or ErrForbidden.
func (r *RBAC) Authorize(p auth.Principal, action string) error {
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
func Require[C, R any](policy Policy, action string) cqrs.Behavior[C, R] {
	return func(next cqrs.HandlerFunc[C, R]) cqrs.HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			var zero R
			p, ok := auth.From(ctx)
			if !ok {
				return zero, ErrUnauthenticated
			}
			if err := policy.Authorize(p, action); err != nil {
				return zero, err
			}
			return next(ctx, cmd)
		}
	}
}
