package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/outbox/gen"
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

// StampChainHeaders returns msg with the correlation/causation ids from ctx
// merged into its JSON Headers — exactly the stamping Enqueue persists.
// Malformed Message.Headers are passed through untouched (the kafka publisher
// already tolerates and drops them — a poison row must not fail the enqueue
// path either).
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
	out, err := json.Marshal(h)
	if err != nil {
		msg.Headers = []byte("{}") // unreachable for map[string]string; defensive
		return msg
	}
	msg.Headers = out
	return msg
}
