// Package transport contains the Kafka consumer transport for the
// notifications service. Transport is DECODE + DISPATCH only: handlers here
// decode records (via consume.Typed) and dispatch to the domain layer; the
// notification rules (what gets sent per outcome) live in
// internal/domain/notification.Service.
package transport

import (
	"context"

	"go-boilerplate/examples/notifications/internal/domain/notification"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/storage/pg"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event-type header values consumed from the payments.events topic
// (derived from the proto messages via consume.EventTypeFor).
var (
	// PaymentProcessedEventType is the success branch of the payment choreography.
	PaymentProcessedEventType = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	// PaymentFailedEventType is the failure branch (declined/failed payments).
	PaymentFailedEventType = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
)

// Notifier is the delivery function for payment notifications — an alias of
// the domain type, kept here so the service-level option
// (notifications.WithNotifier) keeps its historical signature.
type Notifier = notification.Notifier

// NewEventHandler returns a kafka.HandlerFunc that decodes PaymentProcessed
// and PaymentFailed events from the record, deduplicates via the inbox, and
// dispatches to the domain service exactly once per message. All
// transport-level concerns (event-type dispatch, message-id policy, inbox
// dedup, principal headers) come from consume.Typed; the per-outcome
// notification rules live in notification.Service.
func NewEventHandler(pool *pg.Pool, svc *notification.Service, opts ...consume.Option) kafka.HandlerFunc {
	return consume.New(pool, "notifications", opts...).Handler(
		consume.TypedFor(1, func(_ context.Context, evt *ordersv1.PaymentProcessed) error {
			svc.PaymentProcessed(evt.GetOrderId(), evt.GetPaymentId(), evt.GetStatus())
			return nil
		}),
		consume.TypedFor(1, func(_ context.Context, evt *ordersv1.PaymentFailed) error {
			svc.PaymentFailed(evt.GetOrderId())
			return nil
		}),
	)
}
