package transport_test

import (
	"context"
	"errors"
	"testing"

	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/examples/payments/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// publishOrderCreated routes an OrderCreated event through the broker's
// outbox-publisher path — the same path the production relay uses — so the
// record carries the standard event-type/message-id headers.
func publishOrderCreated(t *testing.T, b *fakes.Broker, evt *ordersv1.OrderCreated) error {
	t.Helper()
	payload, err := proto.Marshal(evt)
	require.NoError(t, err)
	return b.Publish(context.Background(), outbox.Message{
		ID:            uuid.New(),
		Topic:         "orders.events",
		AggregateType: "order",
		AggregateID:   evt.GetOrderId(),
		EventType:     transport.OrderCreatedEventType,
		Payload:       payload,
	})
}

// TestNewEventHandler_DecodesAndDispatches drives the REAL transport pipeline
// (header dispatch → proto decode → command mapping) through fakes.Broker,
// no Docker. consume.WithoutInbox is the documented test-only escape hatch.
func TestNewEventHandler_DecodesAndDispatches(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	var got []app.ProcessPayment
	handler := transport.NewEventHandler(nil, func(_ context.Context, cmd app.ProcessPayment) (app.ProcessPaymentResult, error) {
		got = append(got, cmd)
		return app.ProcessPaymentResult{PaymentID: "p1", Status: "processed"}, nil
	}, consume.WithoutInbox())
	broker.Subscribe("orders.events", handler)

	require.NoError(t, publishOrderCreated(t, broker, &ordersv1.OrderCreated{
		OrderId: "order-7", CustomerId: "cust-1", AmountCents: 4200, Currency: "EUR",
	}))

	require.Len(t, got, 1)
	assert.Equal(t, app.ProcessPayment{OrderID: "order-7", AmountCents: 4200, Currency: "EUR"}, got[0],
		"every event field must survive decode → command mapping")
}

// TestNewEventHandler_SkipsUnknownAndPropagatesErrors pins the two transport
// edge behaviors: unknown event types are skipped (forward compatible, no
// error → committed) and handler errors propagate (uncommitted → redelivery).
func TestNewEventHandler_SkipsUnknownAndPropagatesErrors(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	boom := errors.New("transient db failure")
	calls := 0
	handler := transport.NewEventHandler(nil, func(context.Context, app.ProcessPayment) (app.ProcessPaymentResult, error) {
		calls++
		return app.ProcessPaymentResult{}, boom
	}, consume.WithoutInbox())
	broker.Subscribe("orders.events", handler)

	// Unknown event type → skipped, no handler call, nil error.
	require.NoError(t, broker.Publish(context.Background(), outbox.Message{
		ID: uuid.New(), Topic: "orders.events", EventType: "orders.SomethingNew.v2",
	}))
	assert.Zero(t, calls, "unknown event types must be skipped, not dispatched")

	// Handler error → propagated to the consumer loop (triggers redelivery).
	err := publishOrderCreated(t, broker, &ordersv1.OrderCreated{OrderId: "order-8", AmountCents: 1, Currency: "USD"})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, 1, calls)
}
