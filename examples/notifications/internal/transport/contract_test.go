package transport_test

// Consumer half of the payments-outcome contracts (-short, no Docker) for
// the notifications service: canonical fixtures from examples/e2e/contract
// ride the REAL outboxkafka record build through fakes.Broker into the REAL
// transport pipeline + domain service; the producer half (payments'
// contract_test) pins that payments emits exactly these fixtures.

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/examples/e2e/contract"
	"go-boilerplate/examples/notifications/internal/domain/notification"
	"go-boilerplate/examples/notifications/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContract_PaymentOutcomesToNotifier asserts the field-level mapping
// from the canonical wire events to the delivered notification:
//
//   - PaymentProcessed forwards order id, payment id and status verbatim;
//   - PaymentFailed delivers (order id, no payment id, "failed") — the
//     machine-readable reason stays in the event by design.
func TestContract_PaymentOutcomesToNotifier(t *testing.T) {
	t.Parallel()
	type delivered struct{ orderID, paymentID, status string }
	var got []delivered
	svc := notification.NewService(func(orderID, paymentID, status string) {
		got = append(got, delivered{orderID, paymentID, status})
	})
	handler := transport.NewEventHandler(nil, svc, consume.WithoutInbox())

	broker := fakes.NewBroker()
	broker.Subscribe(contract.PaymentsEventsTopic, handler)

	paidOrder, paymentID := uuid.NewString(), uuid.NewString()
	failedOrder := uuid.NewString()
	occurredAt := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	require.NoError(t, broker.Produce(context.Background(),
		contract.WireRecord(t, contract.PaymentProcessed(t, paidOrder, paymentID))))
	require.NoError(t, broker.Produce(context.Background(),
		contract.WireRecord(t, contract.PaymentFailed(t, failedOrder, "declined", occurredAt))))

	require.Len(t, got, 2, "each wire event must deliver exactly one notification")
	assert.Equal(t, delivered{paidOrder, paymentID, "processed"}, got[0],
		"PaymentProcessed → notifier field mapping")
	assert.Equal(t, delivered{failedOrder, "", "failed"}, got[1],
		"PaymentFailed → notifier mapping (no payment id, terminal status)")
}
