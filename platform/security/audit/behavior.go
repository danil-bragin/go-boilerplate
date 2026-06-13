package audit

import (
	"context"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/security/tenant"
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
			// Record the originating tenant when one is in scope. It rides in
			// the metadata map, which is already part of the hash chain, so no
			// schema or chain change is needed and the tenant attribution is
			// itself tamper-evident.
			if tid, ok := tenant.FromContext(ctx); ok {
				entry.Metadata = map[string]string{"tenant_id": tid}
			}
			if recErr := store.Record(ctx, entry); recErr != nil {
				var zero R
				return zero, recErr
			}
			return res, nil
		}
	}
}

// AsyncAudit is the Eventual-consistency twin of Audit: after the handler
// succeeds it builds the same Entry and hands it to the async BufferedAuditWriter
// instead of recording inside the command tx. The command never waits for, nor
// fails on, the audit write (best-effort — a full buffer drops with a metric).
// Use it for high-volume low-value commands (ConsistencyPolicy.SyncAudit=false);
// keep Audit for money/order flows.
func AsyncAudit[C, R any](w *BufferedAuditWriter, action string, subjectFor func(C) string) cqrs.Behavior[C, R] {
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
			// Record the originating tenant when one is in scope. It rides in
			// the metadata map, which is already part of the hash chain, so no
			// schema or chain change is needed and the tenant attribution is
			// itself tamper-evident.
			if tid, ok := tenant.FromContext(ctx); ok {
				entry.Metadata = map[string]string{"tenant_id": tid}
			}
			w.Enqueue(entry) // best-effort; drop+metric on overflow
			return res, nil
		}
	}
}

// DurableAudit is the durable third audit mode (docs/superpowers/specs/2026-06-13-durable-audit-design.md):
// after the handler succeeds it stages the same Entry as Audit into audit_pending
// on the command's transaction (a cheap insert, NO chain-head lock), and a
// single-active per-shard drain worker (servicekit.AddAuditDrain) applies it to
// the hash chain asynchronously and exactly-once. Unlike AsyncAudit it NEVER
// drops (the intent commits with the command); unlike Audit it does not hold the
// chain lock on the hot path (lifting the Strong order-create ceiling). It
// REQUIRES a Transactional command — the intent must commit atomically with the
// command; a staging error rolls the command back (the durability contract).
func DurableAudit[C, R any](store *PgStore, action string, subjectFor func(C) string) cqrs.Behavior[C, R] {
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
			// Record the originating tenant when one is in scope. It rides in
			// the metadata map, which is already part of the hash chain, so no
			// schema or chain change is needed and the tenant attribution is
			// itself tamper-evident.
			if tid, ok := tenant.FromContext(ctx); ok {
				entry.Metadata = map[string]string{"tenant_id": tid}
			}
			if perr := store.InsertPending(ctx, entry); perr != nil {
				var zero R
				return zero, perr
			}
			return res, nil
		}
	}
}
