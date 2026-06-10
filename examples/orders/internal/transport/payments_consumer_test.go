package transport_test

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/examples/orders/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// fakeApplier captures ApplyPaymentOutcome dispatches.
type fakeApplier struct {
	calls []appliedOutcome
	err   error
}

type appliedOutcome struct {
	orderID string
	outcome order.Status
}

func (f *fakeApplier) ApplyPaymentOutcome(_ context.Context, orderID string, outcome order.Status) error {
	f.calls = append(f.calls, appliedOutcome{orderID, outcome})
	return f.err
}

// TestNewPaymentsEventHandler_DecodeAndDispatchOnly pins the consumer as a
// pure decode+dispatch adapter: each payment event maps to exactly one
// ApplyPaymentOutcome call with the matching status — no business branching
// (state machine, first-outcome-wins, compensation warn) remains in
// transport; all of that lives in order.Service and is tested there.
func TestNewPaymentsEventHandler_DecodeAndDispatchOnly(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	applier := &fakeApplier{}
	handler := transport.NewPaymentsEventHandler(nil, applier,
		slog.New(slog.DiscardHandler), consume.WithoutInbox())
	broker.Subscribe("payments.events", handler)

	publish := func(eventType string, msg proto.Message) error {
		payload, err := proto.Marshal(msg)
		require.NoError(t, err)
		return broker.Publish(context.Background(), outbox.Message{
			ID:        uuid.New(),
			Topic:     "payments.events",
			EventType: eventType,
			Payload:   payload,
		})
	}

	paidID, failedID := uuid.NewString(), uuid.NewString()
	require.NoError(t, publish(transport.PaymentProcessedEventType,
		&ordersv1.PaymentProcessed{OrderId: paidID, PaymentId: uuid.NewString(), Status: "processed"}))
	require.NoError(t, publish(transport.PaymentFailedEventType,
		&ordersv1.PaymentFailed{OrderId: failedID, Reason: "declined"}))

	assert.Equal(t, []appliedOutcome{
		{paidID, order.StatusPaid},
		{failedID, order.StatusPaymentFailed},
	}, applier.calls, "PaymentProcessed → paid, PaymentFailed → payment_failed, one dispatch each")

	// Unknown event types on the topic are skipped, not dispatched.
	require.NoError(t, publish("payments.PaymentRefunded.v9", &ordersv1.PaymentProcessed{}))
	assert.Len(t, applier.calls, 2)
}

// TestNewPaymentsEventHandler_PropagatesDomainError pins that transport does
// not swallow service errors: redelivery/DLT policy must see them.
func TestNewPaymentsEventHandler_PropagatesDomainError(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	applier := &fakeApplier{err: assert.AnError}
	handler := transport.NewPaymentsEventHandler(nil, applier,
		slog.New(slog.DiscardHandler), consume.WithoutInbox())
	broker.Subscribe("payments.events", handler)

	payload, err := proto.Marshal(&ordersv1.PaymentProcessed{OrderId: uuid.NewString()})
	require.NoError(t, err)
	err = broker.Publish(context.Background(), outbox.Message{
		ID:        uuid.New(),
		Topic:     "payments.events",
		EventType: transport.PaymentProcessedEventType,
		Payload:   payload,
	})
	require.ErrorIs(t, err, assert.AnError)
}
