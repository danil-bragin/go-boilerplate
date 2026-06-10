package app

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestPaymentOutcome_ThresholdRule pins the demo decline rule that drives the
// choreography failure path: amounts at or above DeclineThresholdCents are
// declined (status "failed" + PaymentFailed event), everything below is
// processed (PaymentProcessed). Pure decision — no DB, no broker.
func TestPaymentOutcome_ThresholdRule(t *testing.T) {
	t.Parallel()
	paymentID := uuid.New()

	tests := []struct {
		name        string
		amountCents int64
		wantStatus  string
	}{
		{"below threshold is processed", DeclineThresholdCents - 1, "processed"},
		{"at threshold is declined", DeclineThresholdCents, "failed"},
		{"above threshold is declined", DeclineThresholdCents + 1, "failed"},
		{"small amount is processed", 1, "processed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := ProcessPayment{OrderID: "order-1", AmountCents: tt.amountCents, Currency: "USD"}
			status, eventType, event := paymentOutcome(cmd, paymentID)

			assert.Equal(t, tt.wantStatus, status)
			switch tt.wantStatus {
			case "processed":
				assert.Equal(t, PaymentProcessedEventType, eventType)
				evt, ok := event.(*ordersv1.PaymentProcessed)
				require.True(t, ok, "processed outcome must emit PaymentProcessed")
				assert.Equal(t, "order-1", evt.GetOrderId())
				assert.Equal(t, paymentID.String(), evt.GetPaymentId())
				assert.Equal(t, "processed", evt.GetStatus())
			case "failed":
				assert.Equal(t, PaymentFailedEventType, eventType)
				evt, ok := event.(*ordersv1.PaymentFailed)
				require.True(t, ok, "declined outcome must emit PaymentFailed")
				assert.Equal(t, "order-1", evt.GetOrderId())
				assert.Equal(t, "declined", evt.GetReason())
				assert.NotNil(t, evt.GetOccurredAt(), "failure event carries its occurrence time")
			}
		})
	}
}
