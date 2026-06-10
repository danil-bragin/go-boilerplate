// Package app contains the CQRS command handlers for the payments service.
package app

import (
	"context"
	"fmt"

	"go-boilerplate/examples/payments/internal/store/gen"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event types emitted by the payments service on payments.events
// (derived from the proto messages via consume.EventTypeFor).
var (
	PaymentProcessedEventType = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	PaymentFailedEventType    = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
)

// DeclineThresholdCents is the deterministic demo decline rule: payments with
// amount_cents >= this threshold are declined. It gives the choreography a
// reproducible failure path (PaymentFailed) without external dependencies —
// any order of 10 000.00 currency units or more fails payment.
const DeclineThresholdCents = 1_000_000

// ProcessPayment is the command to process a payment for an order.
type ProcessPayment struct {
	OrderID     string `validate:"required"`
	AmountCents int64  `validate:"gt=0"`
	Currency    string `validate:"required"`
}

// ProcessPaymentResult is the result of a successful ProcessPayment command.
// Status is "processed" for accepted payments and "failed" for declined ones —
// a decline is a valid domain outcome, not a handler error (errors trigger
// redelivery; a deterministic decline would just fail again).
type ProcessPaymentResult struct {
	PaymentID string
	Status    string
}

// ProcessPaymentHandler returns a cqrs.HandlerFunc that writes a payment row
// and enqueues a PaymentProcessed (or PaymentFailed, for amounts at or above
// DeclineThresholdCents) event to the outbox. It must be called within an
// ambient transaction (e.g. from inbox.ProcessOnce) so that pg.FromContext
// returns the active transaction.
func ProcessPaymentHandler(pool *pg.Pool, outboxRepo *outbox.Repository) cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult] {
	return func(ctx context.Context, cmd ProcessPayment) (ProcessPaymentResult, error) {
		paymentID := uuid.New()
		status, eventType, event := paymentOutcome(cmd, paymentID)

		q := gen.New(pg.FromContext(ctx, pool))
		if err := q.InsertPayment(ctx, gen.InsertPaymentParams{
			ID:          paymentID,
			OrderID:     cmd.OrderID,
			AmountCents: cmd.AmountCents,
			Status:      status,
		}); err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: insert payment: %w", err)
		}

		protoBytes, err := proto.Marshal(event)
		if err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: marshal event: %w", err)
		}

		if err := outboxRepo.Enqueue(ctx, outbox.Message{
			ID:            uuid.New(),
			Topic:         "payments.events",
			AggregateType: "payment",
			AggregateID:   cmd.OrderID,
			EventType:     eventType,
			Payload:       protoBytes,
		}); err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: enqueue event: %w", err)
		}

		return ProcessPaymentResult{PaymentID: paymentID.String(), Status: status}, nil
	}
}

// paymentOutcome is the pure payment decision: it maps a command to the
// resulting payment status, versioned event type, and event payload. Amounts
// at or above DeclineThresholdCents are declined (PaymentFailed — the
// choreography failure branch); everything else is processed.
func paymentOutcome(cmd ProcessPayment, paymentID uuid.UUID) (status, eventType string, event proto.Message) {
	if cmd.AmountCents >= DeclineThresholdCents {
		return "failed", PaymentFailedEventType, &ordersv1.PaymentFailed{
			OrderId:    cmd.OrderID,
			Reason:     "declined",
			OccurredAt: timestamppb.Now(),
		}
	}
	return "processed", PaymentProcessedEventType, &ordersv1.PaymentProcessed{
		OrderId:   cmd.OrderID,
		PaymentId: paymentID.String(),
		Status:    "processed",
	}
}

// DecorateProcessPaymentHandler wraps the raw handler with the standard
// pipeline (Tracing → Logging → Metrics → Validation) plus the Audit behavior.
//
// WithTransaction is intentionally NOT used: the consumer runs this handler
// inside inbox.ProcessOnce, which owns the transaction (see the
// cqrs.Pipeline.WithTransaction godoc). The Audit behavior uses pg.FromContext
// and therefore joins the inbox transaction automatically.
func DecorateProcessPaymentHandler(
	handler cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult],
	auditStore audit.Store,
) cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult] {
	return cqrs.StandardPipeline[ProcessPayment, ProcessPaymentResult]("ProcessPayment").
		Use(audit.Audit[ProcessPayment, ProcessPaymentResult](auditStore, "payment:process", func(cmd ProcessPayment) string {
			return cmd.OrderID
		})).
		Decorate(handler)
}
