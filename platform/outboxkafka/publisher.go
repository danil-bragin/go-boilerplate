// Package outboxkafka bridges the transactional outbox to Kafka: it implements
// outbox.Publisher by producing each outbox Message to Kafka, keyed by the
// aggregate id (preserving per-aggregate ordering within a partition).
//
// Message.Headers must be a JSON object (map[string]string). If the value is
// not a valid JSON object the custom headers are silently dropped and only the
// standard headers (event-type, message-id, aggregate-type) are sent. This is
// intentional: a malformed-header row must not wedge the relay — the message
// is still delivered with degraded metadata rather than blocking the entire
// outbox batch forever (head-of-line block on a poison row).
package outboxkafka

import (
	"context"
	"encoding/json"

	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/outbox"
)

// KafkaPublisher implements outbox.Publisher.
type KafkaPublisher struct {
	producer *kafka.Producer
	topicFor func(outbox.Message) string
}

// Option configures a KafkaPublisher.
type Option func(*KafkaPublisher)

// WithTopicFunc overrides how a Message maps to a topic (default: AggregateType).
func WithTopicFunc(fn func(outbox.Message) string) Option {
	return func(p *KafkaPublisher) { p.topicFor = fn }
}

// New builds a KafkaPublisher. By default the topic is the message's
// AggregateType; override with WithTopicFunc.
func New(producer *kafka.Producer, opts ...Option) *KafkaPublisher {
	p := &KafkaPublisher{
		producer: producer,
		topicFor: func(m outbox.Message) string { return m.AggregateType },
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Publish produces the message to Kafka. Standard headers (event-type,
// message-id, aggregate-type) are added alongside any Message.Headers.
func (p *KafkaPublisher) Publish(ctx context.Context, msg outbox.Message) error {
	headers := map[string]string{
		"event-type":     msg.EventType,
		"message-id":     msg.ID.String(),
		"aggregate-type": msg.AggregateType,
	}
	if len(msg.Headers) > 0 {
		var custom map[string]string
		if err := json.Unmarshal(msg.Headers, &custom); err != nil {
			// Malformed headers must not block the relay. A poison row with
			// invalid JSON in the headers column would otherwise roll back
			// every ProcessBatch attempt and stall the entire outbox forever
			// (head-of-line block). The message is still delivered with only
			// the standard headers; the malformed metadata is dropped.
			//
			// TODO(SP5): replace the silent drop with a structured log warning
			// once the platform logger is wired into this package.
			_ = err // intentionally ignored: proceed with standard headers only
		} else {
			for k, v := range custom {
				headers[k] = v
			}
		}
	}
	return p.producer.Produce(ctx, kafka.Record{
		Topic:   p.topicFor(msg),
		Key:     []byte(msg.AggregateID),
		Value:   msg.Payload,
		Headers: headers,
	})
}
