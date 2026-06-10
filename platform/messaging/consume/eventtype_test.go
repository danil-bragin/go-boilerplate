package consume_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestEventTypeFor_WireCompat pins the derived event-type names to the EXACT
// strings every service already produces and consumes. These are wire values:
// records in flight carry them in the event-type header, so the derivation
// must never change for an existing (message, version) pair.
func TestEventTypeFor_WireCompat(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"CreateOrderCommand", consume.EventTypeFor[*ordersv1.CreateOrderCommand](1), "orders.CreateOrderCommand.v1"},
		{"OrderCreated", consume.EventTypeFor[*ordersv1.OrderCreated](1), "orders.OrderCreated.v1"},
		{"PaymentProcessed", consume.EventTypeFor[*ordersv1.PaymentProcessed](1), "orders.PaymentProcessed.v1"},
		{"PaymentFailed", consume.EventTypeFor[*ordersv1.PaymentFailed](1), "orders.PaymentFailed.v1"},
		{"OrderPaymentTimedOut", consume.EventTypeFor[*ordersv1.OrderPaymentTimedOut](1), "orders.OrderPaymentTimedOut.v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got, "derived event type must equal the existing wire literal")
		})
	}
}

// TestEventTypeFor_Versions: the version parameter names the suffix; a v2
// event of the same message is a distinct wire name.
func TestEventTypeFor_Versions(t *testing.T) {
	assert.Equal(t, "orders.OrderCreated.v2", consume.EventTypeFor[*ordersv1.OrderCreated](2))
	assert.Equal(t, "orders.OrderCreated.v12", consume.EventTypeFor[*ordersv1.OrderCreated](12))
}

// TestEventTypeFor_PackageWithoutVersionSegment: when the proto package has
// no trailing ".vN" segment, the package is used as-is — the version suffix
// still comes from the version parameter. google.protobuf.Timestamp is a
// convenient stand-in for an unversioned package.
func TestEventTypeFor_PackageWithoutVersionSegment(t *testing.T) {
	assert.Equal(t, "google.protobuf.Timestamp.v1", consume.EventTypeFor[*timestamppb.Timestamp](1))
}

// TestTyped_OnCommittedOption: the post-commit callback is supplied via the
// consume.OnCommitted TypedOption and runs after a successful handle.
func TestTyped_OnCommittedOption(t *testing.T) {
	var handled, committed int
	h := consume.New(nil, "g", consume.WithoutInbox()).Handler(
		consume.TypedFor(
			1,
			func(_ context.Context, _ *ordersv1.OrderCreated) error {
				handled++
				return nil
			},
			consume.OnCommitted(func(_ context.Context, evt *ordersv1.OrderCreated) {
				committed++
				assert.Equal(t, "o-2", evt.GetOrderId())
			}),
		),
	)

	payload, err := proto.Marshal(&ordersv1.OrderCreated{OrderId: "o-2"})
	require.NoError(t, err)
	err = h(context.Background(), kafka.Record{
		Topic: "orders.events",
		Value: payload,
		Headers: map[string]string{
			kafka.HeaderEventType: "orders.OrderCreated.v1",
			kafka.HeaderMessageID: "m-2",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, handled)
	assert.Equal(t, 1, committed, "OnCommitted option callback must run after the handler")
}

// TestTypedFor_DispatchesOnDerivedEventType: TypedFor registers the handler
// under the derived name, so a record carrying the existing wire literal in
// its event-type header is dispatched to it.
func TestTypedFor_DispatchesOnDerivedEventType(t *testing.T) {
	var got *ordersv1.OrderCreated
	h := consume.New(nil, "g", consume.WithoutInbox()).Handler(
		consume.TypedFor[*ordersv1.OrderCreated](1, func(_ context.Context, evt *ordersv1.OrderCreated) error {
			got = evt
			return nil
		}),
	)

	payload, err := proto.Marshal(&ordersv1.OrderCreated{OrderId: "o-1"})
	require.NoError(t, err)

	err = h(context.Background(), kafka.Record{
		Topic: "orders.events",
		Value: payload,
		Headers: map[string]string{
			kafka.HeaderEventType: "orders.OrderCreated.v1", // the literal as produced on the wire today
			kafka.HeaderMessageID: "m-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got, "handler must receive the record dispatched via the derived event type")
	assert.Equal(t, "o-1", got.GetOrderId())
}
