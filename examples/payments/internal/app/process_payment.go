// Package app contains the CQRS command handlers for the payments service.
//
// # Layering
//
// Handlers here are THIN ADAPTERS: they map the command to domain-service
// parameters and attach the cross-cutting pipeline (tracing, logging,
// metrics, validation, audit). The business rules — the decline decision,
// persistence, event emission — live in internal/domain/payment.Service.
// Command handlers never call other command handlers ("cmd never calls
// cmd"); shared logic moves into the domain service.
package app

import (
	"context"

	"go-boilerplate/examples/payments/internal/domain/payment"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/audit"
)

// ProcessPayment is the command to process a payment for an order.
// Struct-tag validation is enforced by the cqrs Validation behavior in the
// standard pipeline.
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

// ProcessPaymentHandler adapts the ProcessPayment command to
// payment.Service.Process. It must be called within an ambient transaction
// (e.g. from inbox.ProcessOnce) so the payment row and the outcome event
// commit atomically — see the payment.Service.Process godoc.
func ProcessPaymentHandler(svc *payment.Service) cqrs.HandlerFunc[ProcessPayment, ProcessPaymentResult] {
	return func(ctx context.Context, cmd ProcessPayment) (ProcessPaymentResult, error) {
		res, err := svc.Process(ctx, payment.ProcessParams{
			OrderID:     cmd.OrderID,
			AmountCents: cmd.AmountCents,
			Currency:    cmd.Currency,
		})
		if err != nil {
			return ProcessPaymentResult{}, err
		}
		return ProcessPaymentResult{PaymentID: res.PaymentID, Status: res.Status}, nil
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
