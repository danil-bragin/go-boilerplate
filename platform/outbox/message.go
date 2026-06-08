// Package outbox implements the transactional outbox pattern: events are
// written to an outbox table within the same transaction as the business
// data (Repository.Enqueue), and a Relay later publishes them to a transport
// via the Publisher interface. The transport (e.g. Kafka) is injected; this
// package does not depend on any broker.
package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Message is one outbox record to be published.
type Message struct {
	ID            uuid.UUID
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
