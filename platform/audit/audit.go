// Package audit provides an audit-log store and a CQRS behavior that
// records successful command executions atomically with the command's own
// database transaction.
//
// # Pipeline ordering
//
// The Audit behavior MUST be placed INSIDE the Transaction behavior in the
// pipeline so that the audit INSERT runs within the command's transaction:
//
//	cqrs.Decorate(handler,
//	    cqrs.Transaction[C, R](pool), // outermost — opens the tx
//	    audit.Audit[C, R](store, action, subjectFn), // inner — uses the tx
//	)
//
// If Audit is placed outside Transaction the audit row will be written in a
// separate connection and will NOT roll back when the command tx rolls back.
//
// # Failure policy
//
// Only successful commands are audited. When the handler returns an error the
// audit entry is NOT written (the failure is the caller's concern). However,
// if Record itself returns an error the whole operation is aborted: the error
// is returned to the caller and the surrounding transaction (if any) is rolled
// back. This guarantees that a command whose audit trail cannot be persisted
// never commits its business writes.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/pg"
)

// Entry is a single audit-log record.
type Entry struct {
	Actor    string
	Action   string
	Subject  string
	Metadata map[string]string
	At       time.Time
}

// Store persists audit entries.
type Store interface {
	Record(ctx context.Context, e Entry) error
}

// PgStore writes audit entries to the audit_log table via pg.FromContext so
// that the INSERT participates in the ambient command transaction (if any).
type PgStore struct {
	pool    *pg.Pool
	onError func(error)
}

// NewPgStore returns a PgStore backed by pool.
func NewPgStore(pool *pg.Pool) *PgStore {
	return &PgStore{pool: pool}
}

// SetOnError registers a callback invoked for each Cleanup error inside
// RunCleanup. The loop continues regardless of individual cleanup errors.
func (s *PgStore) SetOnError(fn func(error)) {
	s.onError = fn
}

// Record inserts an audit entry. It uses pg.FromContext so the write joins
// any transaction that is active on ctx (set by pg.RunInTx / cqrs.Transaction).
func (s *PgStore) Record(ctx context.Context, e Entry) error {
	metaJSON, err := marshalMetadata(e.Metadata)
	if err != nil {
		return fmt.Errorf("audit: marshal metadata: %w", err)
	}
	_, err = pg.FromContext(ctx, s.pool).Exec(ctx,
		`insert into audit_log (actor, action, subject, metadata) values ($1, $2, $3, $4)`,
		e.Actor, e.Action, e.Subject, metaJSON,
	)
	if err != nil {
		return fmt.Errorf("audit: record: %w", err)
	}
	return nil
}

// marshalMetadata converts a string map (possibly nil) to a JSON byte slice.
// A nil map produces the JSON literal "{}".
func marshalMetadata(m map[string]string) ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

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
