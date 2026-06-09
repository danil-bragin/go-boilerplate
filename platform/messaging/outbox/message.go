// Package outbox implements the transactional outbox pattern: events are
// written to an outbox table within the same transaction as the business
// data (Repository.Enqueue), and a Relay later publishes them to a transport
// via the Publisher interface. The transport (e.g. Kafka) is injected; this
// package does not depend on any broker.
//
// Delivery semantics are AT-LEAST-ONCE: if Publish succeeds but the
// transaction's commit fails, the message is re-published on a later poll.
// Consumers must be idempotent and deduplicate by Message.ID.
package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Message is one outbox record to be published. Delivery is AT-LEAST-ONCE;
// consumers must deduplicate by ID.
type Message struct {
	ID uuid.UUID
	// Topic is the destination topic for the message (e.g. "orders.events").
	// AggregateType is the domain aggregate kind (e.g. "order") — it is NOT a
	// topic name; publishers fall back to it for legacy rows enqueued before
	// the topic column existed.
	Topic         string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Headers       []byte // JSON object; defaults to {} when nil
	CreatedAt     time.Time
}

// Publisher delivers an outbox message to a transport. Implemented by the
// Kafka adapter in a later sub-project. It must be safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// BatchPublisher is an optional extension of Publisher for transports that
// support efficient batch delivery (e.g. Kafka with async enqueue + single
// Flush). The Relay uses BatchPublisher when the injected Publisher also
// implements this interface; otherwise it falls back to looping Publish.
//
// PublishBatch must deliver all msgs or return an error. Partial delivery is
// not defined: callers treat a non-nil error as "nothing published" and leave
// all rows unpublished for retry on the next poll cycle.
type BatchPublisher interface {
	Publisher
	PublishBatch(ctx context.Context, msgs []Message) error
}
