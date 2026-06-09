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
//
// # Schema Registry framing
//
// When constructed WithEncoder (typically serde.Serde, enabled via the
// SERDE_SR_URL config), record values are framed in the Confluent wire format
// (magic byte + schema id + message index) before the proto payload. Without
// an encoder values are the raw proto bytes — the boilerplate stays runnable
// with no Schema Registry. Producers and consumers must agree on the framing:
// enable SERDE_SR_URL on both sides or neither.
package outboxkafka

import (
	"context"
	"encoding/json"
	"fmt"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
)

// ValueEncoder frames an already proto-marshalled payload for the wire, keyed
// by the versioned event type. Implemented by serde.Serde (Confluent SR wire
// format). The event type must have been registered with the encoder at
// startup; an unregistered type is a configuration error and fails the
// publish (the row stays unpublished and alerts via relay error metrics).
type ValueEncoder interface {
	EncodeValue(eventType string, payload []byte) ([]byte, error)
}

// KafkaPublisher implements outbox.Publisher.
type KafkaPublisher struct {
	producer *kafka.Producer
	topicFor func(outbox.Message) string
	encoder  ValueEncoder
}

// Option configures a KafkaPublisher.
type Option func(*KafkaPublisher)

// WithTopicFunc overrides how a Message maps to a topic (default: Message.Topic,
// falling back to AggregateType for rows enqueued before the Topic field existed).
func WithTopicFunc(fn func(outbox.Message) string) Option {
	return func(p *KafkaPublisher) { p.topicFor = fn }
}

// WithEncoder enables Schema Registry wire-format framing of record values
// (see package doc). enc is typically a *serde.Serde with every produced
// event type registered.
func WithEncoder(enc ValueEncoder) Option {
	return func(p *KafkaPublisher) { p.encoder = enc }
}

// New builds a KafkaPublisher. By default the topic is the message's Topic
// field (AggregateType as legacy fallback); override with WithTopicFunc.
func New(producer *kafka.Producer, opts ...Option) *KafkaPublisher {
	p := &KafkaPublisher{
		producer: producer,
		topicFor: func(m outbox.Message) string {
			if m.Topic != "" {
				return m.Topic
			}
			return m.AggregateType
		},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// messageToRecord converts an outbox.Message to a kafka.Record, applying the
// publisher's topic mapping, optional SR wire framing, and merging standard +
// custom headers. Malformed Message.Headers JSON causes only the standard
// headers to be used (see head-of-line-block rationale in Publish).
func (p *KafkaPublisher) messageToRecord(msg outbox.Message) (kafka.Record, error) {
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
	value := msg.Payload
	if p.encoder != nil {
		framed, err := p.encoder.EncodeValue(msg.EventType, msg.Payload)
		if err != nil {
			return kafka.Record{}, fmt.Errorf("outboxkafka: encode %s (id=%s): %w", msg.EventType, msg.ID, err)
		}
		value = framed
	}
	return kafka.Record{
		Topic:   p.topicFor(msg),
		Key:     []byte(msg.AggregateID),
		Value:   value,
		Headers: headers,
	}, nil
}

// PublishBatch produces all messages to Kafka via a single batched flush,
// reducing broker round-trips from O(N) to O(1). It implements
// outbox.BatchPublisher and is used by the Relay when available.
func (p *KafkaPublisher) PublishBatch(ctx context.Context, msgs []outbox.Message) error {
	records := make([]kafka.Record, len(msgs))
	for i, msg := range msgs {
		rec, err := p.messageToRecord(msg)
		if err != nil {
			return err
		}
		records[i] = rec
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
	rec, err := p.messageToRecord(msg)
	if err != nil {
		return err
	}
	return p.producer.Produce(ctx, rec)
}
