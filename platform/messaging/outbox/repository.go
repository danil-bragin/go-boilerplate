package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/outbox/gen"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/security/tenant"
	"go-boilerplate/platform/storage/pg"
)

// Repository persists outbox messages using the transaction bound to the
// context (via pg.RunInTx), so an enqueue commits atomically with the
// business data written in the same transaction.
type Repository struct {
	pool *pg.Pool
}

// NewRepository creates a Repository over the given pool.
func NewRepository(pool *pg.Pool) *Repository {
	return &Repository{pool: pool}
}

// Enqueue inserts a message into the outbox using the context's DBTX.
// Message.Topic defaults to Message.AggregateType when empty (legacy callers
// that encoded the topic in the aggregate type).
//
// Chain lineage is stamped automatically (see platform/messaging/msgctx):
// "correlation-id" is taken from ctx (set by consume.Typed), defaulting to
// the message's own id when this message starts a new chain; "causation-id"
// is the parent message id from ctx when present. Explicit values already in
// Message.Headers always win — stamping never overwrites.
func (r *Repository) Enqueue(ctx context.Context, msg Message) error {
	headers := StampChainHeaders(ctx, msg).Headers
	topic := msg.Topic
	if topic == "" {
		topic = msg.AggregateType
	}
	q := gen.New(pg.FromContext(ctx, r.pool))
	err := q.InsertOutbox(ctx, gen.InsertOutboxParams{
		ID:            msg.ID,
		Topic:         topic,
		AggregateType: msg.AggregateType,
		AggregateID:   msg.AggregateID,
		EventType:     msg.EventType,
		Payload:       msg.Payload,
		Headers:       json.RawMessage(headers),
	})
	if err != nil {
		return fmt.Errorf("outbox: enqueue: %w", err)
	}
	return nil
}

// StampChainHeaders returns msg with the correlation/causation ids AND the
// authenticated principal (principal-sub / principal-roles) from ctx merged
// into its JSON Headers — exactly the stamping Enqueue persists. Stamping the
// principal lets downstream consumers (consume.Typed → auth.ExtractToContext)
// attribute their audit trails to the ORIGINATING actor across an arbitrary
// number of event hops, not just the first edge→service hop.
//
// Existing header values always win — stamping never overwrites — so an
// explicit principal-sub/principal-roles in Message.Headers is preserved.
//
// Malformed Message.Headers are passed through untouched (the kafka publisher
// already tolerates and drops them — a poison row must not fail the enqueue
// path either).
//
// SECURITY NOTE: principal-sub/principal-roles are transport metadata, not
// authentication — see auth.InjectHeaders. The trust boundary is the broker
// ACL/mTLS perimeter (round-8 A1 adds real SASL/TLS controls).
//
// Exported as a seam for fast-lane contract tests (examples/e2e/contract)
// that exercise the producer→consumer chain-lineage roundtrip without a
// database; production code goes through Enqueue.
func StampChainHeaders(ctx context.Context, msg Message) Message {
	h := map[string]string{}
	if len(msg.Headers) > 0 {
		if err := json.Unmarshal(msg.Headers, &h); err != nil {
			return msg // not a JSON object — leave as-is
		}
	}
	if _, ok := h[msgctx.HeaderCorrelationID]; !ok {
		if corr := msgctx.CorrelationID(ctx); corr != "" {
			h[msgctx.HeaderCorrelationID] = corr
		} else {
			// This message starts a new chain: correlate to itself so every
			// downstream event still shares one chain id.
			h[msgctx.HeaderCorrelationID] = msg.ID.String()
		}
	}
	if _, ok := h[msgctx.HeaderCausationID]; !ok {
		if parent := msgctx.ParentMessageID(ctx); parent != "" {
			h[msgctx.HeaderCausationID] = parent
		}
	}
	// Principal propagation: InjectHeaders is a no-op when ctx carries no
	// principal and (per its contract) sets principal-sub/principal-roles only
	// when absent is not its concern — it always overwrites — so guard the
	// keys ourselves to honour the "explicit headers win" rule.
	if _, ok := h[auth.HeaderPrincipalSub]; !ok {
		auth.InjectHeaders(ctx, h)
	}
	// Tenant propagation: stamp the originating tenant so downstream consumers
	// (consume.Typed → tenant.ExtractToContext) stay scoped to it across hops.
	// Same "explicit headers win" rule as the principal above.
	if _, ok := h[tenant.HeaderTenantID]; !ok {
		tenant.InjectHeaders(ctx, h)
	}
	out, err := json.Marshal(h)
	if err != nil {
		msg.Headers = []byte("{}") // unreachable for map[string]string; defensive
		return msg
	}
	msg.Headers = out
	return msg
}
