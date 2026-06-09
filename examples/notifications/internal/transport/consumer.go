// Package transport contains the Kafka consumer transport for the notifications service.
package transport

import (
	"context"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event-type header values consumed from the payments.events topic.
const (
	// PaymentProcessedEventType is the success branch of the payment choreography.
	PaymentProcessedEventType = "orders.PaymentProcessed.v1"
	// PaymentFailedEventType is the failure branch (declined/failed payments).
	PaymentFailedEventType = "orders.PaymentFailed.v1"
)

// Notifier is a function called when a payment notification should be sent.
// The default implementation logs a structured line; tests can inject a
// capturing implementation to assert invocations. For failed payments the
// paymentID is empty (no payment was created) and status is "failed".
type Notifier func(orderID, paymentID, status string)

// NewEventHandler returns a kafka.HandlerFunc that decodes PaymentProcessed
// and PaymentFailed events from the record, deduplicates via the inbox, and
// invokes the notifier exactly once per message. All transport-level concerns
// (event-type dispatch, message-id policy, inbox dedup, principal headers)
// come from consume.Typed.
func NewEventHandler(pool *pg.Pool, notifier Notifier, opts ...consume.Option) kafka.HandlerFunc {
	return consume.New(pool, "notifications", opts...).Handler(
		consume.Typed(PaymentProcessedEventType, func(_ context.Context, evt *ordersv1.PaymentProcessed) error {
			notifier(evt.GetOrderId(), evt.GetPaymentId(), evt.GetStatus())
			return nil
		}),
		consume.Typed(PaymentFailedEventType, func(_ context.Context, evt *ordersv1.PaymentFailed) error {
			// Failure notification: no payment id exists; the status is the
			// terminal "failed" outcome (the reason travels in the event and
			// is logged by the default notifier wiring).
			notifier(evt.GetOrderId(), "", "failed")
			return nil
		}),
	)
}
