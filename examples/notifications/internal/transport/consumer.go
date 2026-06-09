// Package transport contains the Kafka consumer transport for the notifications service.
package transport

import (
	"context"

	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// PaymentProcessedEventType is the versioned event-type header value for
// PaymentProcessed records on the payments.events topic.
const PaymentProcessedEventType = "orders.PaymentProcessed.v1"

// Notifier is a function called when a payment notification should be sent.
// The default implementation logs a structured line; tests can inject a
// capturing implementation to assert invocations.
type Notifier func(orderID, paymentID, status string)

// NewEventHandler returns a kafka.HandlerFunc that decodes a PaymentProcessed
// event from the record, deduplicates via the inbox, and invokes the notifier
// exactly once per message. All transport-level concerns (event-type dispatch,
// message-id policy, inbox dedup, principal headers) come from consume.Typed.
func NewEventHandler(pool *pg.Pool, notifier Notifier, opts ...consume.Option) kafka.HandlerFunc {
	return consume.New(pool, "notifications", opts...).Handler(
		consume.Typed(PaymentProcessedEventType, func(_ context.Context, evt *ordersv1.PaymentProcessed) error {
			notifier(evt.GetOrderId(), evt.GetPaymentId(), evt.GetStatus())
			return nil
		}),
	)
}
