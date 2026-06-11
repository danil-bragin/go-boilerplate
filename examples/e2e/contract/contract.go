// Package contract holds the canonical cross-service event fixtures for the
// fast-lane (-short, no Docker) contract tests: the exact outbox.Message each
// producer is expected to emit on the wire for every event that crosses a
// service boundary.
//
// The fixtures are the "pact" linking two test halves that the Go compiler
// forbids from living in one package (a service's internal/ tree is only
// importable from inside that service):
//
//   - PRODUCER tests (in the producing service's own tree) run the real
//     domain service against capturing fakes and assert the captured message
//     equals the canonical fixture (RequireSameMessage) — if the producer
//     stops populating a field, renames an event, or changes the key choice,
//     that test breaks;
//   - CONSUMER tests (in the consuming service's own tree) feed the canonical
//     fixture through the REAL produce-side wire mapping (WireRecord — the
//     outboxkafka record build) and fakes.Broker into the real consumer
//     pipeline, asserting field-level expectations — if the consumer maps a
//     field wrongly or drops a header, that test breaks.
//
// Together the halves pin the semantic contract that buf-breaking cannot see
// (a field that stops being populated, a key change, a dropped header),
// with the fixture as the single drift detector in between.
//
// This package must stay importable from every service's _test files: only
// exported, non-internal dependencies (outbox, kafka, outboxkafka, consume,
// gen/proto) — and no platform/testkit imports (the arch guard keeps testkit
// out of non-test packages; fakes.Broker usage belongs in the _test files).
package contract

import (
	"testing"
	"time"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/outboxkafka"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Topics the example services exchange events on. Each side pins its own
// config default against these in its contract test, so a one-sided topic
// rename fails fast instead of silently splitting the choreography.
const (
	// OrdersEventsTopic carries OrderCreated / OrderPaymentTimedOut
	// (produced by orders; consumed by gateway projection and payments).
	OrdersEventsTopic = "orders.events"
	// PaymentsEventsTopic carries PaymentProcessed / PaymentFailed
	// (produced by payments; consumed by gateway projection and notifications).
	PaymentsEventsTopic = "payments.events"
)

// OrderCreated is the canonical OrderCreated message: what orders'
// Service.Create must enqueue for the given inputs.
func OrderCreated(tb testing.TB, orderID, customerID string, amountCents int64, currency string) outbox.Message {
	tb.Helper()
	return message(tb, OrdersEventsTopic, "order", orderID,
		consume.EventTypeFor[*ordersv1.OrderCreated](1),
		&ordersv1.OrderCreated{
			OrderId:     orderID,
			CustomerId:  customerID,
			AmountCents: amountCents,
			Currency:    currency,
		})
}

// OrderPaymentTimedOut is the canonical payment-timeout message: what orders'
// Service.EmitPaymentTimeout must enqueue when its claim wins.
func OrderPaymentTimedOut(tb testing.TB, orderID string, deadline time.Time) outbox.Message {
	tb.Helper()
	return message(tb, OrdersEventsTopic, "order", orderID,
		consume.EventTypeFor[*ordersv1.OrderPaymentTimedOut](1),
		&ordersv1.OrderPaymentTimedOut{
			OrderId:  orderID,
			Deadline: timestamppb.New(deadline),
		})
}

// PaymentProcessed is the canonical success-outcome message: what payments'
// Service.Process must enqueue for a below-threshold amount. Note the key
// (AggregateID) is the ORDER id, not the payment id — per-order ordering on
// the topic is part of the contract.
func PaymentProcessed(tb testing.TB, orderID, paymentID string) outbox.Message {
	tb.Helper()
	return message(tb, PaymentsEventsTopic, "payment", orderID,
		consume.EventTypeFor[*ordersv1.PaymentProcessed](1),
		&ordersv1.PaymentProcessed{
			OrderId:   orderID,
			PaymentId: paymentID,
			Status:    "processed",
		})
}

// PaymentFailed is the canonical decline-outcome message: what payments'
// Service.Process must enqueue at/above the decline threshold. occurredAt is
// the producer's injected clock reading.
func PaymentFailed(tb testing.TB, orderID, reason string, occurredAt time.Time) outbox.Message {
	tb.Helper()
	return message(tb, PaymentsEventsTopic, "payment", orderID,
		consume.EventTypeFor[*ordersv1.PaymentFailed](1),
		&ordersv1.PaymentFailed{
			OrderId:    orderID,
			Reason:     reason,
			OccurredAt: timestamppb.New(occurredAt),
		})
}

// WireRecord maps msg to the kafka.Record the relay would produce, via the
// REAL outboxkafka record build (topic mapping, aggregate-id key, standard +
// custom headers; no Schema Registry framing — the no-SR default both sides
// of the fast lane decode). Feed the result to fakes.Broker.Produce to drive
// a consumer with exactly what production puts on the wire.
func WireRecord(tb testing.TB, msg outbox.Message) kafka.Record {
	tb.Helper()
	rec, err := outboxkafka.New(nil).Record(msg)
	require.NoError(tb, err, "contract: outboxkafka record build")
	return rec
}

// RequireSameMessage asserts that a producer's captured message carries the
// same wire contract as the canonical fixture, field by field: topic,
// aggregate type, aggregate id (the partition key), versioned event type,
// and a payload that decodes to a proto-equal T. Message.ID is ignored
// (random per enqueue), as are Headers (stamped later by outbox.Enqueue —
// covered by the chain-lineage roundtrip test in this package).
func RequireSameMessage[T proto.Message](tb testing.TB, want, got outbox.Message) {
	tb.Helper()
	require.Equal(tb, want.Topic, got.Topic, "topic")
	require.Equal(tb, want.AggregateType, got.AggregateType, "aggregate type")
	require.Equal(tb, want.AggregateID, got.AggregateID, "aggregate id (partition key)")
	require.Equal(tb, want.EventType, got.EventType, "versioned event type")
	require.NotEqual(tb, uuid.Nil, got.ID, "message id must be set (inbox dedup identity)")

	wantEvt, gotEvt := decode[T](tb, want.Payload), decode[T](tb, got.Payload)
	require.Empty(tb, cmpProto(wantEvt, gotEvt),
		"payload drift:\nwant: %v\ngot:  %v", wantEvt, gotEvt)
}

func decode[T proto.Message](tb testing.TB, payload []byte) T {
	tb.Helper()
	var zero T
	msg, ok := zero.ProtoReflect().Type().New().Interface().(T)
	require.True(tb, ok, "contract: cannot instantiate %T", zero)
	require.NoError(tb, proto.Unmarshal(payload, msg), "contract: payload must decode as %T", zero)
	return msg
}

// cmpProto returns "" when the messages are proto-equal, else a marker the
// require.Empty above surfaces with both renderings.
func cmpProto(a, b proto.Message) string {
	if proto.Equal(a, b) {
		return ""
	}
	return "not proto-equal"
}

// message marshals the event and wraps it in the canonical outbox envelope.
func message(tb testing.TB, topic, aggregateType, aggregateID, eventType string, evt proto.Message) outbox.Message {
	tb.Helper()
	payload, err := proto.Marshal(evt)
	require.NoError(tb, err, "contract: marshal %s", eventType)
	return outbox.Message{
		ID:            uuid.New(),
		Topic:         topic,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
	}
}
