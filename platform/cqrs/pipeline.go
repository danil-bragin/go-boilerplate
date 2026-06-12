package cqrs

import (
	"context"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/pg"
)

// ErrUnauthenticated is returned by the WithAuthz pipeline option when no
// principal is present in the context. It is an apperr error carrying
// AUTH_UNAUTHENTICATED, so httpx.FromError maps it to 401 automatically;
// errors.Is(err, cqrs.ErrUnauthenticated) keeps working (code equality).
var ErrUnauthenticated error = apperr.New(apperr.CodeAuthUnauthenticated)

// AuthzPolicy decides whether a principal may perform an action on a resource.
// It is structurally identical to platform/security/authz.Policy (declared
// here because authz imports cqrs — the reverse import would cycle), so any
// authz.Policy implementation (RBAC, ownership checks) satisfies it directly.
type AuthzPolicy interface {
	Authorize(ctx context.Context, p auth.Principal, action string, resource any) error
}

// Pipeline is a fluent builder for the standard CQRS behavior stack. Construct
// it with StandardPipeline, chain options, then call Decorate (or Behaviors).
//
// The assembled order is canonical regardless of option call order:
//
//	Tracing (outermost) → Logging → Metrics → Deadline? → Validation →
//	Authz? → Caching? → Transaction? → Use()'d behaviors (innermost) → handler
//
// Tracing outermost is load-bearing: every inner behavior (and the handler)
// runs inside the span so context-aware log records carry trace_id/span_id —
// see the package doc.
//
// Resilience (retries, circuit breaking, rate limiting) is deliberately NOT a
// pipeline concern: it stays at the transport level (httpserver middleware,
// kafka/retry escalation, platform/resilience around outbound calls), where
// the failure domain and backoff semantics are known.
type Pipeline[C, R any] struct {
	name       string
	deadline   time.Duration
	authz      AuthzPolicy
	action     string
	cache      Cache
	keyFor     func(C) string
	ttl        time.Duration
	txPool     *pg.Pool
	usesOutbox bool
	extra      []Behavior[C, R]
}

// StandardPipeline starts a Pipeline named name with the core stack
// (Tracing → Logging → Metrics → Validation). Typical call sites collapse to
// one expression:
//
//	h := cqrs.StandardPipeline[GetOrder, OrderView]("GetOrder").
//	    WithCache(cache, keyFor, 30*time.Second).
//	    Decorate(raw)
func StandardPipeline[C, R any](name string) *Pipeline[C, R] {
	return &Pipeline[C, R]{name: name}
}

// WithDeadline bounds handler execution with the Deadline behavior (placed
// after Metrics, before Validation).
func (p *Pipeline[C, R]) WithDeadline(d time.Duration) *Pipeline[C, R] {
	p.deadline = d
	return p
}

// WithAuthz authorizes the ctx principal for action before the handler runs
// (after Validation). The command is passed to the policy as the resource. A
// missing principal yields ErrUnauthenticated; a policy denial propagates the
// policy's error (e.g. one wrapping authz.ErrForbidden).
func (p *Pipeline[C, R]) WithAuthz(policy AuthzPolicy, action string) *Pipeline[C, R] {
	p.authz = policy
	p.action = action
	return p
}

// WithCache adds the CachingJSON behavior (QUERIES ONLY — never combine with
// WithTransaction; cached results must be side-effect free).
func (p *Pipeline[C, R]) WithCache(cache Cache, keyFor func(C) string, ttl time.Duration) *Pipeline[C, R] {
	p.cache = cache
	p.keyFor = keyFor
	p.ttl = ttl
	return p
}

// WithTransaction wraps the handler in pg.RunInTx (COMMANDS ONLY).
//
// Do NOT use it for handlers invoked from a Kafka consumer built on
// inbox.ProcessOnce: the inbox owns the transaction there — it opens its own
// RunInTx and the handler already runs inside it. Adding WithTransaction
// would create a redundant savepoint (pgx v5 opens a SAVEPOINT on the
// already-open tx) — not incorrect, but unnecessary. Behaviors that use
// pg.FromContext (e.g. Audit) join the inbox transaction automatically.
func (p *Pipeline[C, R]) WithTransaction(pool *pg.Pool) *Pipeline[C, R] {
	p.txPool = pool
	return p
}

// WithOutbox marks this command as one that enqueues to the transactional
// outbox. It makes the consistency guard reject Transactional:false (which would
// break effectively-once). Set it on every outbox-producing command's pipeline.
func (p *Pipeline[C, R]) WithOutbox() *Pipeline[C, R] {
	p.usesOutbox = true
	return p
}

// WithConsistency applies the policy's Transactional axis: when true it wraps the
// handler in the Transaction behavior (pg.RunInTx) using pool; when false it does
// not. The SyncAudit/SyncRYW/SyncProjection axes are honoured by the caller
// (audit.Audit vs audit.AsyncAudit, gateway pending/projection wiring) because
// cqrs must not import audit. Panics if Transactional is false on a command
// marked WithOutbox — that combination silently breaks effectively-once.
func (p *Pipeline[C, R]) WithConsistency(policy ConsistencyPolicy, pool *pg.Pool) *Pipeline[C, R] {
	if !policy.Transactional && p.usesOutbox {
		panic("cqrs: ConsistencyPolicy{Transactional:false} on a WithOutbox command breaks effectively-once (the order-write + outbox-enqueue would not be atomic)")
	}
	if policy.Transactional {
		p.txPool = pool
	}
	return p
}

// Use appends domain behaviors (Audit, custom checks, …) innermost — they run
// closest to the handler, inside every standard behavior.
func (p *Pipeline[C, R]) Use(behaviors ...Behavior[C, R]) *Pipeline[C, R] {
	p.extra = append(p.extra, behaviors...)
	return p
}

// Behaviors assembles the behavior list in canonical order (outermost first),
// suitable for Decorate.
func (p *Pipeline[C, R]) Behaviors() []Behavior[C, R] {
	bs := []Behavior[C, R]{
		Tracing[C, R](p.name),
		Logging[C, R](p.name),
		Metrics[C, R](p.name),
	}
	if p.deadline > 0 {
		bs = append(bs, Deadline[C, R](p.deadline))
	}
	bs = append(bs, Validation[C, R]())
	if p.authz != nil {
		bs = append(bs, authorize[C, R](p.authz, p.action))
	}
	if p.cache != nil {
		bs = append(bs, CachingJSON[C, R](p.cache, p.keyFor, p.ttl))
	}
	if p.txPool != nil {
		bs = append(bs, Transaction[C, R](p.txPool))
	}
	return append(bs, p.extra...)
}

// Decorate wraps h with the assembled pipeline.
func (p *Pipeline[C, R]) Decorate(h HandlerFunc[C, R]) HandlerFunc[C, R] {
	return Decorate(h, p.Behaviors()...)
}

// authorize is the WithAuthz behavior: principal lookup + policy check.
func authorize[C, R any](policy AuthzPolicy, action string) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			var zero R
			pr, ok := auth.From(ctx)
			if !ok {
				return zero, ErrUnauthenticated
			}
			if err := policy.Authorize(ctx, pr, action, cmd); err != nil {
				return zero, err
			}
			return next(ctx, cmd)
		}
	}
}
