package transport_test

// Consumer half of the OrderCreated contract (-short, no Docker): the
// canonical fixture from examples/e2e/contract rides the REAL outboxkafka
// record build (contract.WireRecord) through fakes.Broker into the REAL
// transport pipeline; the producer half (orders' contract_test) pins that
// orders emits exactly this fixture. consume.WithoutInbox is the documented
// test-only escape hatch — decode, dispatch and command mapping are the
// production code paths.

import (
	"context"
	"testing"

	"go-boilerplate/examples/e2e/contract"
	"go-boilerplate/examples/payments/internal/app"
	"go-boilerplate/examples/payments/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContract_OrderCreatedToProcessPayment asserts the field-level mapping
// from the canonical wire event to the ProcessPayment command: order id,
// amount and currency must survive decode → dispatch exactly (customer_id is
// deliberately NOT forwarded — payments has no use for it).
func TestContract_OrderCreatedToProcessPayment(t *testing.T) {
	t.Parallel()
	orderID := uuid.NewString()
	msg := contract.OrderCreated(t, orderID, "cust-42", 123_456, "EUR")

	var got []app.ProcessPayment
	handler := transport.NewEventHandler(nil, func(_ context.Context, cmd app.ProcessPayment) (app.ProcessPaymentResult, error) {
		got = append(got, cmd)
		return app.ProcessPaymentResult{PaymentID: "p1", Status: "processed"}, nil
	}, consume.WithoutInbox())

	broker := fakes.NewBroker()
	broker.Subscribe(msg.Topic, handler) // subscribed where the producer publishes
	require.NoError(t, broker.Produce(context.Background(), contract.WireRecord(t, msg)))

	require.Len(t, got, 1, "one wire event must dispatch exactly one command")
	assert.Equal(t, app.ProcessPayment{
		OrderID:     orderID,
		AmountCents: 123_456,
		Currency:    "EUR",
	}, got[0], "OrderCreated → ProcessPayment field mapping")
}
