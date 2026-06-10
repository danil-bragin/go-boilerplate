package notification_test

import (
	"testing"

	"go-boilerplate/examples/notifications/internal/domain/notification"

	"github.com/stretchr/testify/assert"
)

type sent struct{ orderID, paymentID, status string }

// TestService_PaymentOutcomes pins the notification rules for both
// choreography branches: success forwards the payment identity verbatim;
// failure notifies with NO payment id (none was created) and the terminal
// "failed" status regardless of the event's internal reason.
func TestService_PaymentOutcomes(t *testing.T) {
	t.Parallel()

	var got []sent
	svc := notification.NewService(func(orderID, paymentID, status string) {
		got = append(got, sent{orderID, paymentID, status})
	})

	svc.PaymentProcessed("order-1", "pay-1", "processed")
	svc.PaymentFailed("order-2")

	assert.Equal(t, []sent{
		{"order-1", "pay-1", "processed"},
		{"order-2", "", "failed"},
	}, got)
}
