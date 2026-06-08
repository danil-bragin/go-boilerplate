// Package outboxkafka bridges the transactional outbox to Kafka: it implements
// outbox.Publisher by producing each outbox Message to Kafka, keyed by the
// aggregate id (preserving per-aggregate ordering within a partition).
package outboxkafka

import (
	"context"
	"encoding/json"
	"fmt"

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
			return fmt.Errorf("outboxkafka: parse headers: %w", err)
		}
		for k, v := range custom {
			headers[k] = v
		}
	}
	return p.producer.Produce(ctx, kafka.Record{
		Topic:   p.topicFor(msg),
		Key:     []byte(msg.AggregateID),
		Value:   msg.Payload,
		Headers: headers,
	})
}
