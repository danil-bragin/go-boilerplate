package transport_test

import (
	"context"
	"testing"

	"go-boilerplate/examples/notifications/internal/domain/notification"
	"go-boilerplate/examples/notifications/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

type notified struct{ orderID, paymentID, status string }

// TestNewEventHandler_NotifiesPerOutcome drives the REAL notifications
// pipeline (header dispatch → proto decode → notifier call) through
// fakes.Broker for both choreography branches — success and failure.
// consume.WithoutInbox is the documented test-only escape hatch.
func TestNewEventHandler_NotifiesPerOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventType string
		event     proto.Message
		want      notified
	}{
		{
			name:      "PaymentProcessed notifies with payment id and status",
			eventType: transport.PaymentProcessedEventType,
			event:     &ordersv1.PaymentProcessed{OrderId: "order-1", PaymentId: "pay-1", Status: "processed"},
			want:      notified{orderID: "order-1", paymentID: "pay-1", status: "processed"},
		},
		{
			name:      "PaymentFailed notifies with empty payment id and failed status",
			eventType: transport.PaymentFailedEventType,
			event:     &ordersv1.PaymentFailed{OrderId: "order-2", Reason: "declined"},
			want:      notified{orderID: "order-2", paymentID: "", status: "failed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			broker := fakes.NewBroker()
			var got []notified
			handler := transport.NewEventHandler(nil, notification.NewService(func(orderID, paymentID, status string) {
				got = append(got, notified{orderID, paymentID, status})
			}), consume.WithoutInbox())
			broker.Subscribe("payments.events", handler)

			payload, err := proto.Marshal(tt.event)
			require.NoError(t, err)
			require.NoError(t, broker.Publish(context.Background(), outbox.Message{
				ID:        uuid.New(),
				Topic:     "payments.events",
				EventType: tt.eventType,
				Payload:   payload,
			}))

			require.Len(t, got, 1)
			assert.Equal(t, tt.want, got[0])
		})
	}
}

// TestNewEventHandler_SkipsUnknownEventTypes pins forward compatibility: a
// new producer-side event type must not break (or notify from) old consumers.
func TestNewEventHandler_SkipsUnknownEventTypes(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()
	count := 0
	handler := transport.NewEventHandler(nil, notification.NewService(func(_, _, _ string) {
		count++
	}), consume.WithoutInbox())
	broker.Subscribe("payments.events", handler)

	require.NoError(t, broker.Publish(context.Background(), outbox.Message{
		ID: uuid.New(), Topic: "payments.events", EventType: "orders.PaymentRefunded.v1",
	}))
	assert.Zero(t, count, "unknown event types must be skipped silently")
}
