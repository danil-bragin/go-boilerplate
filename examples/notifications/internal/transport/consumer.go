// Package transport contains the Kafka consumer transport for the notifications service.
package transport

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
	"go-boilerplate/platform/inbox"
	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/pg"
)

// Notifier is a function called when a payment notification should be sent.
// The default implementation logs a structured line; tests can inject a
// capturing implementation to assert invocations.
type Notifier func(orderID, paymentID, status string)

// NewEventHandler returns a kafka.HandlerFunc that decodes a PaymentProcessed
// event from the record, deduplicates via inbox.ProcessOnce, and invokes the
// notifier exactly once per (orderID, paymentID) pair.
//
// Message-ID derivation: the Kafka header "message-id" is used when present
// (set by the payments service outbox relay). Otherwise the message ID is
// derived as "<orderID>-<paymentID>", so the same PaymentProcessed event
// (same order and payment) is deduplicated by the inbox table even without
// the header — giving effectively-once notification under at-least-once
// Kafka delivery.
func NewEventHandler(pool *pg.Pool, notifier Notifier) kafka.HandlerFunc {
	return func(ctx context.Context, r kafka.Record) error {
		var evt ordersv1.PaymentProcessed
		if err := proto.Unmarshal(r.Value, &evt); err != nil {
			return fmt.Errorf("notifications consumer: unmarshal event: %w", err)
		}

		// Derive the inbox message id.
		msgID := r.Headers["message-id"]
		if msgID == "" {
			if evt.GetOrderId() == "" || evt.GetPaymentId() == "" {
				return fmt.Errorf("notifications consumer: cannot derive message id: no message-id header and order_id/payment_id are empty")
			}
			msgID = evt.GetOrderId() + "-" + evt.GetPaymentId()
		}

		_, err := inbox.ProcessOnce(ctx, pool, "notifications", msgID, func(_ context.Context) error {
			notifier(evt.GetOrderId(), evt.GetPaymentId(), evt.GetStatus())
			return nil
		})
		return err
	}
}
