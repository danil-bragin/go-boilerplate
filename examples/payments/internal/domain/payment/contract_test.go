package payment_test

// Producer half of the cross-service contract tests (-short, no Docker) for
// the payments outcome events: the REAL payment.Service decides + enqueues,
// and the captured message is asserted equal to the canonical fixture in
// examples/e2e/contract (the consumer halves — gateway projection and
// notifications transport — drive the same fixtures; see the contract
// package doc).

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go-boilerplate/examples/e2e/contract"
	"go-boilerplate/examples/payments"
	"go-boilerplate/examples/payments/internal/domain/payment"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestContract_PaymentProcessed pins the success outcome: keyed by the ORDER
// id (per-order topic ordering), payment_id from the created row, literal
// status "processed".
func TestContract_PaymentProcessed(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	svc := newService(&fakeRepo{}, pub, time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC))

	orderID := uuid.NewString()
	res, err := svc.Process(context.Background(), payment.ProcessParams{
		OrderID:     orderID,
		AmountCents: payment.DeclineThresholdCents - 1, // just below: processed
		Currency:    "USD",
	})
	require.NoError(t, err)
	require.Equal(t, payment.StatusProcessed, res.Status)

	require.Len(t, pub.msgs, 1, "Process must enqueue exactly one event")
	contract.RequireSameMessage[*ordersv1.PaymentProcessed](t,
		contract.PaymentProcessed(t, orderID, res.PaymentID), pub.msgs[0])
}

// TestContract_PaymentFailed pins the decline outcome: reason "declined" and
// occurred_at from the INJECTED clock (deterministic — wall-clock would make
// the event unstable under redelivery).
func TestContract_PaymentFailed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC)
	pub := &fakePublisher{}
	svc := newService(&fakeRepo{}, pub, now)

	orderID := uuid.NewString()
	res, err := svc.Process(context.Background(), payment.ProcessParams{
		OrderID:     orderID,
		AmountCents: payment.DeclineThresholdCents, // at threshold: declined
		Currency:    "USD",
	})
	require.NoError(t, err)
	require.Equal(t, payment.StatusFailed, res.Status)

	require.Len(t, pub.msgs, 1, "Process must enqueue exactly one event")
	contract.RequireSameMessage[*ordersv1.PaymentFailed](t,
		contract.PaymentFailed(t, orderID, "declined", now), pub.msgs[0])
}

// TestContract_TopicDefaults pins the service's PRODUCTION topic defaults
// (the env tags on examples/payments Config) against the shared contract
// constants: OutTopic is where these fixtures say the events land, and
// EventsTopic is where the OrderCreated fixture says payments listens. A
// one-sided default rename silently splits the choreography — this makes it
// a -short failure.
func TestContract_TopicDefaults(t *testing.T) {
	t.Parallel()
	cfgType := reflect.TypeOf(payments.Config{})

	outField, ok := cfgType.FieldByName("OutTopic")
	require.True(t, ok, "payments.Config.OutTopic field")
	require.Equal(t, contract.PaymentsEventsTopic, outField.Tag.Get("envDefault"))

	inField, ok := cfgType.FieldByName("EventsTopic")
	require.True(t, ok, "payments.Config.EventsTopic field")
	require.Equal(t, contract.OrdersEventsTopic, inField.Tag.Get("envDefault"))
}
