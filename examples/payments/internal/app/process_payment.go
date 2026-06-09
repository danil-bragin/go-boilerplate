// Package app contains the CQRS command handlers for the payments service.
package app

import (
	"context"
	"fmt"

	"go-boilerplate/examples/payments/internal/store/gen"
	"go-boilerplate/platform/audit"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// ProcessPayment is the command to process a payment for an order.
type ProcessPayment struct {
	OrderID     string `validate:"required"`
	AmountCents int64  `validate:"gt=0"`
	Currency    string `validate:"required"`
}

// ProcessPaymentResult is the result of a successful ProcessPayment command.
type ProcessPaymentResult struct {
	PaymentID string
}

// ProcessPaymentHandler returns a cqrs.HandlerFunc that writes a payment row
// and enqueues a PaymentProcessed event to the outbox. It must be called within
// an ambient transaction (e.g. from inbox.ProcessOnce) so that pg.FromContext
// returns the active transaction.
func ProcessPaymentHandler(pool *pg.Pool, outboxRepo *outbox.Repository) cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult] {
	return func(ctx context.Context, cmd ProcessPayment) (ProcessPaymentResult, error) {
		paymentID := uuid.New()

		q := gen.New(pg.FromContext(ctx, pool))
		if err := q.InsertPayment(ctx, gen.InsertPaymentParams{
			ID:          paymentID,
			OrderID:     cmd.OrderID,
			AmountCents: cmd.AmountCents,
			Status:      "processed",
		}); err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: insert payment: %w", err)
		}

		event := &ordersv1.PaymentProcessed{
			OrderId:   cmd.OrderID,
			PaymentId: paymentID.String(),
			Status:    "processed",
		}
		protoBytes, err := proto.Marshal(event)
		if err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: marshal event: %w", err)
		}

		if err := outboxRepo.Enqueue(ctx, outbox.Message{
			ID:            uuid.New(),
			AggregateType: "payments.events",
			AggregateID:   cmd.OrderID,
			EventType:     "PaymentProcessed",
			Payload:       protoBytes,
		}); err != nil {
			return ProcessPaymentResult{}, fmt.Errorf("process_payment: enqueue event: %w", err)
		}

		return ProcessPaymentResult{PaymentID: paymentID.String()}, nil
	}
}

// DecorateProcessPaymentHandler wraps the raw handler with Logging, Tracing,
// Metrics, Validation, and Audit behaviors.
//
// NOTE: Transaction behavior is intentionally omitted. The consumer uses
// inbox.ProcessOnce which opens its own RunInTx; the handler runs inside that
// transaction. Adding Transaction here would create a redundant savepoint
// (pgx v5 would open a SAVEPOINT on the already-open tx) — not incorrect, but
// unnecessary. The Audit behavior uses pg.FromContext and therefore joins the
// inbox transaction automatically.
func DecorateProcessPaymentHandler(
	handler cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult],
	auditStore audit.Store,
) cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult] {
	return cqrs.Decorate(
		handler,
		cqrs.Logging[ProcessPayment, ProcessPaymentResult]("ProcessPayment"),
		cqrs.Tracing[ProcessPayment, ProcessPaymentResult]("ProcessPayment"),
		cqrs.Metrics[ProcessPayment, ProcessPaymentResult]("ProcessPayment"),
		cqrs.Validation[ProcessPayment, ProcessPaymentResult](),
		audit.Audit[ProcessPayment, ProcessPaymentResult](auditStore, "payment:process", func(cmd ProcessPayment) string {
			return cmd.OrderID
		}),
	)
}
