package fakes

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
)

// Broker is an in-memory Kafka substitute for fast-lane tests: it implements
// outbox.Publisher / outbox.BatchPublisher (drop-in for the outboxkafka
// publisher) and dispatches produced records synchronously to subscribed
// kafka.HandlerFuncs — including handlers built with consume.Typed.
//
// Semantics (deliberately simpler than a real broker):
//   - single partition 0 per topic, offsets assigned monotonically per topic;
//   - dispatch is SYNCHRONOUS and in-order: Produce returns after every
//     subscriber on the topic ran (handler errors are joined and returned),
//     so tests never need sleeps or polling;
//   - no replay: a subscriber added after a record was produced does not see
//     that record (use Records for assertions on past traffic);
//   - no goroutines — goleak-clean by construction.
//
// Publish maps outbox.Message → kafka.Record exactly like the outboxkafka
// publisher: topic from Message.Topic (AggregateType fallback for legacy
// rows), key = AggregateID, standard headers event-type / message-id /
// aggregate-type, custom JSON headers merged on top, malformed Headers JSON
// silently dropped (standard headers still sent).
//
// Safe for concurrent use. Handlers may produce follow-up records from
// within a dispatch (re-entrant) — choreography chains run to completion in
// one synchronous call.
type Broker struct {
	mu      sync.Mutex
	subs    map[string][]kafka.HandlerFunc
	records map[string][]kafka.Record
	nextOff map[string]int64
}

var (
	_ outbox.Publisher      = (*Broker)(nil)
	_ outbox.BatchPublisher = (*Broker)(nil)
)

// NewBroker returns an empty in-memory broker.
func NewBroker() *Broker {
	return &Broker{
		subs:    make(map[string][]kafka.HandlerFunc),
		records: make(map[string][]kafka.Record),
		nextOff: make(map[string]int64),
	}
}

// Subscribe registers a handler for topic. Multiple handlers per topic are
// allowed (each models one consumer group) and run in registration order.
func (b *Broker) Subscribe(topic string, h kafka.HandlerFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], h)
}

// Produce assigns the record its position (partition 0, next offset on the
// topic), appends it to the topic log, and synchronously dispatches it to
// every subscriber. Errors from subscribers are joined; the record stays in
// the log either way (the broker accepted it — the consumer failed).
func (b *Broker) Produce(ctx context.Context, rec kafka.Record) error {
	b.mu.Lock()
	rec.Partition = 0
	rec.Offset = b.nextOff[rec.Topic]
	b.nextOff[rec.Topic]++
	b.records[rec.Topic] = append(b.records[rec.Topic], rec)
	handlers := make([]kafka.HandlerFunc, len(b.subs[rec.Topic]))
	copy(handlers, b.subs[rec.Topic])
	b.mu.Unlock() // dispatch outside the lock: handlers may re-enter Produce

	var errs []error
	for _, h := range handlers {
		if err := h(ctx, rec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Publish implements outbox.Publisher (see type doc for the mapping).
func (b *Broker) Publish(ctx context.Context, msg outbox.Message) error {
	return b.Produce(ctx, messageToRecord(msg))
}

// PublishBatch implements outbox.BatchPublisher: messages are delivered
// in order via Produce; the first dispatch error aborts the rest (callers
// treat a batch error as "retry the whole batch", matching the relay).
func (b *Broker) PublishBatch(ctx context.Context, msgs []outbox.Message) error {
	for _, msg := range msgs {
		if err := b.Publish(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Records returns a copy of every record produced to topic, in order, with
// their assigned positions.
func (b *Broker) Records(topic string) []kafka.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]kafka.Record, len(b.records[topic]))
	copy(cp, b.records[topic])
	return cp
}

// messageToRecord mirrors outboxkafka's outbox.Message → kafka.Record
// mapping (minus Schema Registry framing, which needs a live registry).
func messageToRecord(msg outbox.Message) kafka.Record {
	topic := msg.Topic
	if topic == "" {
		topic = msg.AggregateType
	}
	headers := map[string]string{
		kafka.HeaderEventType: msg.EventType,
		kafka.HeaderMessageID: msg.ID.String(),
		"aggregate-type":      msg.AggregateType,
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
		Topic:   topic,
		Key:     []byte(msg.AggregateID),
		Value:   msg.Payload,
		Headers: headers,
	}
}
