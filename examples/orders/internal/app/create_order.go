// Package app contains the CQRS command handlers for the orders service.
package app

import (
	"context"
	"fmt"

	"go-boilerplate/examples/orders/internal/store/gen"
	"go-boilerplate/platform/audit"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/outbox"
	"go-boilerplate/platform/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// CreateOrder is the command to create a new order.
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

// CreateOrderHandler returns a cqrs.HandlerFunc that writes an order row and
// enqueues an OrderCreated event to the outbox. It must be called within an
// ambient transaction (e.g. from inbox.ProcessOnce) so that pg.FromContext
// returns the active transaction.
func CreateOrderHandler(pool *pg.Pool, outboxRepo *outbox.Repository) cqrs.HandlerFunc[CreateOrder, CreateOrderResult] {
	return func(ctx context.Context, cmd CreateOrder) (CreateOrderResult, error) {
		orderID, err := uuid.Parse(cmd.OrderID)
		if err != nil {
			return CreateOrderResult{}, fmt.Errorf("create_order: invalid order id: %w", err)
		}

		q := gen.New(pg.FromContext(ctx, pool))
		if err := q.InsertOrder(ctx, gen.InsertOrderParams{
			ID:          orderID,
			CustomerID:  cmd.CustomerID,
			AmountCents: cmd.AmountCents,
			Currency:    cmd.Currency,
			Status:      "created",
		}); err != nil {
			return CreateOrderResult{}, fmt.Errorf("create_order: insert order: %w", err)
		}

		event := &ordersv1.OrderCreated{
			OrderId:     cmd.OrderID,
			CustomerId:  cmd.CustomerID,
			AmountCents: cmd.AmountCents,
			Currency:    cmd.Currency,
		}
		protoBytes, err := proto.Marshal(event)
		if err != nil {
			return CreateOrderResult{}, fmt.Errorf("create_order: marshal event: %w", err)
		}

		if err := outboxRepo.Enqueue(ctx, outbox.Message{
			ID:            uuid.New(),
			AggregateType: "orders.events",
			AggregateID:   cmd.OrderID,
			EventType:     "OrderCreated",
			Payload:       protoBytes,
		}); err != nil {
			return CreateOrderResult{}, fmt.Errorf("create_order: enqueue event: %w", err)
		}

		return CreateOrderResult{OrderID: cmd.OrderID}, nil
	}
}

// DecorateCreateOrderHandler wraps the raw handler with Logging, Tracing,
// Metrics, Validation, and Audit behaviors.
//
// NOTE: Transaction behavior is intentionally omitted. The consumer uses
// inbox.ProcessOnce which opens its own RunInTx; the handler runs inside that
// transaction. Adding Transaction here would create a redundant savepoint
// (pgx v5 would open a SAVEPOINT on the already-open tx) — not incorrect, but
// unnecessary. The Audit behavior uses pg.FromContext and therefore joins the
// inbox transaction automatically.
func DecorateCreateOrderHandler(
	handler cqrs.HandlerFunc[CreateOrder, CreateOrderResult],
	auditStore audit.Store,
) cqrs.HandlerFunc[CreateOrder, CreateOrderResult] {
	return cqrs.Decorate(
		handler,
		cqrs.Logging[CreateOrder, CreateOrderResult]("CreateOrder"),
		cqrs.Tracing[CreateOrder, CreateOrderResult]("CreateOrder"),
		cqrs.Metrics[CreateOrder, CreateOrderResult]("CreateOrder"),
		cqrs.Validation[CreateOrder, CreateOrderResult](),
		audit.Audit[CreateOrder, CreateOrderResult](auditStore, "order:create", func(cmd CreateOrder) string {
			return cmd.OrderID
		}),
	)
}
