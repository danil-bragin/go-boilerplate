// Package payment is the domain layer of the payments service: the payment
// decision rule (decline threshold), the Repository port over storage, and
// the Service that owns the process-payment business flow.
//
// # Layering
//
// Entry points — the CQRS command handler (internal/app) and the Kafka
// transport (internal/transport) — are thin adapters over Service. Command
// handlers never call other command handlers ("cmd never calls cmd"); shared
// logic lives here. The Repository interface is defined CONSUMER-SIDE (next
// to the Service that needs it); the Postgres implementation is in pg.go.
//
// # Error codes
//
// The payments service currently registers no PAYMENTS_* apperr codes: a
// declined payment is a VALID domain outcome (an error would loop redelivery
// on a deterministic decline), and the service has no other domain failure
// modes — input validation is the cqrs Validation behavior's
// VALIDATION_FAILED. When a payments-owned failure mode appears, its
// PAYMENTS_* block belongs in a codes.go here, registered from init() (see
// examples/orders/internal/domain/order/codes.go for the pattern).
package payment

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
)

// Payment is the domain view of a payment row.
type Payment struct {
	ID          uuid.UUID
	OrderID     string
	AmountCents int64
	Status      string
	CreatedAt   time.Time
}

// Repository is the payment persistence port, defined consumer-side.
// Implementations resolve their query surface from the CONTEXT (ambient
// transaction; see PgRepository), so Service writes join whatever
// transaction the caller owns.
type Repository interface {
	// Insert writes a new payment row. CreatedAt is ignored: the row
	// timestamp is DB time (DEFAULT now()).
	Insert(ctx context.Context, p Payment) error

	// GetByOrder returns the payment recorded for an order (storage error
	// when absent).
	GetByOrder(ctx context.Context, orderID string) (Payment, error)
}

// EventPublisher is the outbox port the Service enqueues domain events
// through; *outbox.Repository implements it directly. Atomicity with the
// payment-row write is NOT provided by this interface — it comes from the
// shared ambient transaction: both the Repository and *outbox.Repository
// resolve their DBTX from the same ctx, so enqueue and row write commit (or
// roll back) together whenever the caller runs them under one transaction.
type EventPublisher interface {
	Enqueue(ctx context.Context, msg outbox.Message) error
}
