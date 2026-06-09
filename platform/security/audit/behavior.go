package audit

import (
	"context"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"
)

// actorFrom extracts the actor identifier from the context principal.
// It prefers Principal.Subject; if no principal is present it returns
// "anonymous".
func actorFrom(ctx context.Context) string {
	if p, ok := auth.From(ctx); ok {
		if p.Subject != "" {
			return p.Subject
		}
		if p.Username != "" {
			return p.Username
		}
	}
	return "anonymous"
}

// Audit returns a CQRS behavior that records a successful command execution
// to store. The entry's Actor is taken from the ctx principal (or "anonymous"
// when no principal is present), the Action is the provided action string, and
// the Subject is obtained by calling subjectFor(cmd).
//
// Only successful commands produce an audit entry. If the handler returns an
// error, Record is NOT called. If Record returns an error, that error is
// returned by the behavior and will roll back the surrounding transaction.
//
// See the package-level doc for the required pipeline ordering.
func Audit[C, R any](store Store, action string, subjectFor func(C) string) cqrs.Behavior[C, R] {
	return func(next cqrs.HandlerFunc[C, R]) cqrs.HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			res, err := next(ctx, cmd)
			if err != nil {
				return res, err
			}
			entry := Entry{
				Actor:   actorFrom(ctx),
				Action:  action,
				Subject: subjectFor(cmd),
			}
			if recErr := store.Record(ctx, entry); recErr != nil {
				var zero R
				return zero, recErr
			}
			return res, nil
		}
	}
}
