package cqrs

// ConsistencyPolicy declares, per command type, how strong each consistency
// axis is. Strong is the default; relax explicitly per high-volume flow. See
// docs/superpowers/specs/2026-06-13-a2-consistency-policy-design.md.
type ConsistencyPolicy struct {
	Transactional  bool // wrap the handler in the Transaction behavior (ACID)
	SyncAudit      bool // audit inside the command tx (caller honours this when choosing audit.Audit vs audit.AsyncAudit)
	SyncRYW        bool // synchronous pending-row insert (gateway honours this)
	SyncProjection bool // sync per-event projection (consumer honours this)
}

// Strong is fully ACID — the default for money/order flows.
var Strong = ConsistencyPolicy{Transactional: true, SyncAudit: true, SyncRYW: true, SyncProjection: true}

// Eventual relaxes audit/RYW/projection but KEEPS the transaction (so outbox
// effectively-once is preserved by default). Drop the tx explicitly with
// .With(Transactional(false)) for single-write, no-outbox commands.
var Eventual = ConsistencyPolicy{Transactional: true, SyncAudit: false, SyncRYW: false, SyncProjection: false}

// ConsistencyOption overrides one axis of a ConsistencyPolicy.
type ConsistencyOption func(*ConsistencyPolicy)

// Transactional returns a ConsistencyOption that sets the Transactional axis.
func Transactional(v bool) ConsistencyOption {
	return func(p *ConsistencyPolicy) { p.Transactional = v }
}

// SyncAudit returns a ConsistencyOption that sets the SyncAudit axis.
func SyncAudit(v bool) ConsistencyOption { return func(p *ConsistencyPolicy) { p.SyncAudit = v } }

// SyncRYW returns a ConsistencyOption that sets the SyncRYW axis.
func SyncRYW(v bool) ConsistencyOption { return func(p *ConsistencyPolicy) { p.SyncRYW = v } }

// SyncProjection returns a ConsistencyOption that sets the SyncProjection axis.
func SyncProjection(v bool) ConsistencyOption {
	return func(p *ConsistencyPolicy) { p.SyncProjection = v }
}

// With returns a copy of p with the given axes overridden.
func (p ConsistencyPolicy) With(opts ...ConsistencyOption) ConsistencyPolicy {
	for _, o := range opts {
		o(&p)
	}
	return p
}
