package serde_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "go-boilerplate/gen/proto/events/v1"
	"go-boilerplate/platform/kafka/kafkatest"
	"go-boilerplate/platform/serde"
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
