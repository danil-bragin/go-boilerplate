package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go-boilerplate/platform/messaging/outbox/gen"
	"go-boilerplate/platform/pg"
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
func (r *Repository) Enqueue(ctx context.Context, msg Message) error {
	headers := msg.Headers
	if headers == nil {
		headers = []byte("{}")
	}
	q := gen.New(pg.FromContext(ctx, r.pool))
	err := q.InsertOutbox(ctx, gen.InsertOutboxParams{
		ID:            msg.ID,
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
