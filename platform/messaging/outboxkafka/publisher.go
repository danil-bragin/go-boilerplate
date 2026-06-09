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

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
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

// messageToRecord converts an outbox.Message to a kafka.Record, applying the
// publisher's topic mapping and merging standard + custom headers. Malformed
// Message.Headers JSON causes only the standard headers to be used (see
// head-of-line-block rationale in Publish).
func (p *KafkaPublisher) messageToRecord(msg outbox.Message) kafka.Record {
	headers := map[string]string{
		"event-type":     msg.EventType,
		"message-id":     msg.ID.String(),
		"aggregate-type": msg.AggregateType,
	}
	if len(msg.Headers) > 0 {
		var custom map[string]string
		if err := json.Unmarshal(msg.Headers, &custom); err == nil {
			for k, v := range custom {
				headers[k] = v
			}
		}
	}
	return kafka.Record{
		Topic:   p.topicFor(msg),
		Key:     []byte(msg.AggregateID),
		Value:   msg.Payload,
		Headers: headers,
	}
}

// PublishBatch produces all messages to Kafka via a single batched flush,
// reducing broker round-trips from O(N) to O(1). It implements
// outbox.BatchPublisher and is used by the Relay when available.
func (p *KafkaPublisher) PublishBatch(ctx context.Context, msgs []outbox.Message) error {
	records := make([]kafka.Record, len(msgs))
	for i, msg := range msgs {
		records[i] = p.messageToRecord(msg)
	}
	return p.producer.ProduceBatch(ctx, records)
}

// Publish produces the message to Kafka. Standard headers (event-type,
// message-id, aggregate-type) are added alongside any Message.Headers.
// Malformed Message.Headers JSON is silently dropped so a poison row cannot
// block the relay (head-of-line block prevention).
//
// TODO(SP5): replace the silent drop with a structured log warning once the
// platform logger is wired into this package.
func (p *KafkaPublisher) Publish(ctx context.Context, msg outbox.Message) error {
	return p.producer.Produce(ctx, p.messageToRecord(msg))
}
