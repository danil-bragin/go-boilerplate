// Package app contains the CQRS command handlers for the orders service.
//
// # Layering
//
// Handlers here are THIN ADAPTERS: they map the command to domain-service
// parameters and attach the cross-cutting pipeline (tracing, logging,
// metrics, validation, audit). Every business rule — state machine, first
// outcome wins, exactly-once timeout emission — lives in
// internal/domain/order.Service. Command handlers never call other command
// handlers ("cmd never calls cmd"); when two entry points need the same
// logic, it moves into the domain service, never into a handler-to-handler
// call.
package app

import (
	"context"
	"fmt"
	"math/big"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/money"
	"go-boilerplate/platform/security/audit"
)

// CreateOrder is the command to create a new order. Struct-tag validation is
// enforced by the cqrs Validation behavior in the standard pipeline; domain
// rules (UUID shape, state machine) live in order.Service.
type CreateOrder struct {
	OrderID     string `validate:"required"`
	CustomerID  string `validate:"required"`
	AmountCents int64  `validate:"gt=0"`
	Currency    string `validate:"required"`
}

// CreateOrderResult is the result of a successful CreateOrder command.
type CreateOrderResult struct {
	OrderID string
}

// CreateOrderHandler adapts the CreateOrder command to order.Service.Create.
// It must be called within an ambient transaction (e.g. from
// inbox.ProcessOnce) so the order row and the OrderCreated outbox event
// commit atomically — see the order.Service.Create godoc.
func CreateOrderHandler(svc *order.Service) cqrs.HandlerFunc[CreateOrder, CreateOrderResult] {
	return func(ctx context.Context, cmd CreateOrder) (CreateOrderResult, error) {
		// Convert the wire shape (minor units + currency) into a precision-exact
		// money.Money at the boundary.
		amount, err := money.FromMinor(big.NewInt(cmd.AmountCents), cmd.Currency)
		if err != nil {
			return CreateOrderResult{}, fmt.Errorf("create order: amount %d %s: %w", cmd.AmountCents, cmd.Currency, err)
		}
		if err := svc.Create(ctx, order.CreateParams{
			OrderID:    cmd.OrderID,
			CustomerID: cmd.CustomerID,
			Amount:     amount,
		}); err != nil {
			return CreateOrderResult{}, err
		}
		return CreateOrderResult{OrderID: cmd.OrderID}, nil
	}
}

// DecorateCreateOrderHandler wraps the raw handler with the standard pipeline
// (Tracing → Logging → Metrics → Validation) plus the Audit behavior.
//
// WithTransaction is intentionally NOT used: the consumer runs this handler
// inside inbox.ProcessOnce, which owns the transaction (see the
// cqrs.Pipeline.WithTransaction godoc). The Audit behavior uses pg.FromContext
// and therefore joins the inbox transaction automatically.
func DecorateCreateOrderHandler(
	handler cqrs.HandlerFunc[CreateOrder, CreateOrderResult],
	auditStore audit.Store,
) cqrs.HandlerFunc[CreateOrder, CreateOrderResult] {
	return cqrs.StandardPipeline[CreateOrder, CreateOrderResult]("CreateOrder").
		Use(audit.Audit[CreateOrder, CreateOrderResult](auditStore, "order:create", func(cmd CreateOrder) string {
			return cmd.OrderID
		})).
		Decorate(handler)
}
