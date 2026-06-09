package serde_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/serde"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
	testkitv1 "go-boilerplate/gen/proto/testkit/v1"
)

func TestProtobufSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	s := serde.NewProtobuf()

	original := &ordersv1.OrderCreated{
		OrderId:     "1",
		CustomerId:  "c",
		AmountCents: 100,
		Currency:    "USD",
	}

	encoded, err := s.Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded := &ordersv1.OrderCreated{}
	require.NoError(t, s.Decode(encoded, decoded))

	assert.True(t, proto.Equal(original, decoded), "decoded message must equal original")
}

func TestSerde_RegisterContextCancelled(t *testing.T) {
	t.Parallel()

	s, err := serde.New("http://127.0.0.1:1") // never dialled before Register
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.Register(ctx, "orders.events-value", "orders.OrderCreated.v1", &ordersv1.OrderCreated{})
	require.Error(t, err, "Register must honour the caller context and fail fast")
}

// TestSchemaRegistrySerde is the integration suite for the SR-backed serde.
// One Redpanda container is shared across the subtests; the subtests use
// distinct subjects so they do not interfere.
func TestSchemaRegistrySerde(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	t.Parallel()

	_, srURL := kafkatest.NewRedpanda(t)
	ctx := context.Background()

	client, err := sr.NewClient(sr.URLs(srURL))
	require.NoError(t, err)

	original := &ordersv1.OrderCreated{
		OrderId:     "1",
		CustomerId:  "c",
		AmountCents: 100,
		Currency:    "USD",
	}

	t.Run("wire format and round trip", func(t *testing.T) {
		s, err := serde.New(srURL)
		require.NoError(t, err)
		require.NoError(t, s.Register(ctx, "orders.events-value", "orders.OrderCreated.v1", original))

		encoded, err := s.Encode(original)
		require.NoError(t, err)
		require.Greater(t, len(encoded), 7)

		// Confluent wire format: magic byte 0.
		assert.Equal(t, byte(0x00), encoded[0], "first byte must be the Confluent magic byte 0x00")

		// 4-byte big-endian schema id must match what the registry assigned.
		ss, err := client.SchemaByVersion(ctx, "orders.events-value", -1)
		require.NoError(t, err)
		assert.Equal(t, uint32(ss.ID), binary.BigEndian.Uint32(encoded[1:5]), "schema id mismatch")

		// Protobuf message index: OrderCreated is the SECOND top-level message
		// in orders/v1/orders.proto → index [1] → varint count 1, varint value 1
		// (zig-zag encoded: 0x02 0x02).
		assert.Equal(t, []byte{0x02, 0x02}, encoded[5:7], "message index framing mismatch")

		// Remainder is the plain protobuf payload.
		raw, err := proto.Marshal(original)
		require.NoError(t, err)
		assert.Equal(t, raw, encoded[7:], "payload after header must be plain protobuf")

		decoded := &ordersv1.OrderCreated{}
		require.NoError(t, s.Decode(encoded, decoded))
		assert.True(t, proto.Equal(original, decoded), "decoded message must equal original")
	})

	t.Run("EncodeValue matches Encode", func(t *testing.T) {
		s, err := serde.New(srURL)
		require.NoError(t, err)
		require.NoError(t, s.Register(ctx, "orders.events-value", "orders.OrderCreated.v1", original))

		viaMsg, err := s.Encode(original)
		require.NoError(t, err)

		raw, err := proto.Marshal(original)
		require.NoError(t, err)
		viaRaw, err := s.EncodeValue("orders.OrderCreated.v1", raw)
		require.NoError(t, err)

		assert.Equal(t, viaMsg, viaRaw, "EncodeValue must produce identical wire bytes")

		_, err = s.EncodeValue("orders.Unknown.v1", raw)
		require.Error(t, err, "unregistered event type must error")
	})

	t.Run("registration is idempotent across instances", func(t *testing.T) {
		const subject = "serde-idem-value"
		for range 2 {
			s, err := serde.New(srURL)
			require.NoError(t, err)
			require.NoError(t, s.Register(ctx, subject, "orders.OrderCreated.v1", original))
			// Registering twice on the same instance must also be a no-op.
			require.NoError(t, s.Register(ctx, subject, "orders.OrderCreated.v1", original))
		}
		versions, err := client.SubjectVersions(ctx, subject)
		require.NoError(t, err)
		assert.Len(t, versions, 1, "re-registration must reuse the existing schema version, not create a new one")
	})

	t.Run("unknown schema id yields typed error", func(t *testing.T) {
		s, err := serde.New(srURL)
		require.NoError(t, err)
		require.NoError(t, s.Register(ctx, "orders.events-value", "orders.OrderCreated.v1", original))

		encoded, err := s.Encode(original)
		require.NoError(t, err)

		// Corrupt the schema id to one that is not registered locally.
		bogus := append([]byte(nil), encoded...)
		binary.BigEndian.PutUint32(bogus[1:5], 999999)

		decoded := &ordersv1.OrderCreated{}
		err = s.Decode(bogus, decoded)
		require.ErrorIs(t, err, serde.ErrUnknownSchema)
	})

	t.Run("WKT timestamp round trip", func(t *testing.T) {
		s, err := serde.New(srURL)
		require.NoError(t, err)

		evt := &testkitv1.TimestampedEvent{
			Id:         "1",
			OccurredAt: timestamppb.New(time.Unix(1700000000, 42).UTC()),
			Labels:     map[string]string{"k": "v"},
		}
		require.NoError(t, s.Register(ctx, "testkit.events-value", "testkit.TimestampedEvent.v1", evt))

		encoded, err := s.Encode(evt)
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), encoded[0])

		decoded := &testkitv1.TimestampedEvent{}
		require.NoError(t, s.Decode(encoded, decoded))
		assert.True(t, proto.Equal(evt, decoded), "decoded TimestampedEvent must equal original")
	})
}
