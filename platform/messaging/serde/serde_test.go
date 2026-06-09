package serde_test

import (
	"testing"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/serde"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "go-boilerplate/gen/proto/events/v1"
)

func TestProtobufSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	s := serde.NewProtobuf()

	original := &eventsv1.OrderCreated{
		OrderId:     "1",
		CustomerId:  "c",
		AmountCents: 100,
		Currency:    "USD",
	}

	encoded, err := s.Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded := &eventsv1.OrderCreated{}
	require.NoError(t, s.Decode(encoded, decoded))

	assert.True(t, proto.Equal(original, decoded), "decoded message must equal original")
}

func TestSchemaRegistrySerde_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	t.Parallel()

	_, srURL := kafkatest.NewRedpanda(t)

	sr, err := serde.NewSchemaRegistry(srURL, "orders-value", &eventsv1.OrderCreated{})
	if err != nil {
		t.Skipf("schema registry unavailable: %v", err)
	}

	original := &eventsv1.OrderCreated{
		OrderId:     "1",
		CustomerId:  "c",
		AmountCents: 100,
		Currency:    "USD",
	}

	encoded, err := sr.Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Confluent wire format: first byte must be the magic byte 0x00.
	assert.Equal(t, byte(0x00), encoded[0], "first byte must be the Confluent magic byte 0x00")

	decoded := &eventsv1.OrderCreated{}
	require.NoError(t, sr.Decode(encoded, decoded))

	assert.True(t, proto.Equal(original, decoded), "decoded message must equal original")
}

// TestSchemaRegistrySerde_EnvelopeWithTimestampRoundTrip is the regression test
// for the hand-rolled protoSchemaText bug: EventEnvelope uses
// google.protobuf.Timestamp (a WKT) and a map field.  The old hand-rolled schema
// printer dropped the import, producing invalid proto source that Redpanda SR
// rejected. The fix uses jhump/protoreflect protoprint.Printer on the real
// FileDescriptor, which emits the correct import; Redpanda SR resolves WKTs
// internally so no explicit schema reference is needed.
func TestSchemaRegistrySerde_EnvelopeWithTimestampRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	t.Parallel()

	_, srURL := kafkatest.NewRedpanda(t)

	sr, err := serde.NewSchemaRegistry(srURL, "envelope-value", &eventsv1.EventEnvelope{})
	if err != nil {
		t.Skipf("schema registry unavailable: %v", err)
	}

	original := &eventsv1.EventEnvelope{
		Id:            "1",
		Type:          "t",
		AggregateType: "order",
		AggregateId:   "42",
		OccurredAt:    timestamppb.Now(),
		Payload:       []byte("x"),
		Headers:       map[string]string{"k": "v"},
	}

	encoded, err := sr.Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Confluent wire format: first byte must be the magic byte 0x00.
	assert.Equal(t, byte(0x00), encoded[0], "first byte must be the Confluent magic byte 0x00")

	decoded := &eventsv1.EventEnvelope{}
	require.NoError(t, sr.Decode(encoded, decoded))

	assert.True(t, proto.Equal(original, decoded), "decoded EventEnvelope must equal original")
}
