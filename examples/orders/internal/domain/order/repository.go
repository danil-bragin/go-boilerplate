// Package order is the domain layer of the orders service: the order state
// machine (statemachine.go), the ORDERS_* error codes (codes.go), the
// Repository port over storage, and the Service that owns every business
// rule about orders.
//
// # Layering
//
// Entry points — the CQRS command handler (internal/app), the Kafka
// transport (internal/transport) and the unpaid watcher loop — are thin
// adapters: they decode/dispatch (transport), decorate with the pipeline
// (app), or own the loop+transaction boundary (watcher), and delegate every
// decision to Service. Command handlers never call other command handlers
// ("cmd never calls cmd"); logic two entry points both need lives HERE, in
// the domain service, which is exactly why Service exists.
//
// The Repository interface is defined CONSUMER-SIDE (in this package, next
// to the Service that needs it) per the repository-interfaces-consumer-side
// convention; the Postgres implementation lives in pg.go.
package order

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/money"

	"github.com/google/uuid"
)

// Order is the domain view of an order row. Amount is a precision-exact
// platform/money value (any asset), stored as amount NUMERIC + asset TEXT.
type Order struct {
	ID         uuid.UUID
	CustomerID string
	Amount     money.Money
	Status     Status
	CreatedAt  time.Time
}

// UnpaidOrder is one expired-unpaid candidate returned by ListUnpaidExpired:
// just enough to claim the order and compute the timeout event's deadline.
type UnpaidOrder struct {
	ID        uuid.UUID
	CreatedAt time.Time
}

// Repository is the order persistence port, defined consumer-side: the
// Service declares what it needs, storage adapters (pg.go) satisfy it.
//
// Implementations resolve their query surface from the CONTEXT (ambient
// transaction; see PgRepository), so a Service method called inside a
// transaction has all its writes commit or roll back together with that
// transaction — including the outbox enqueue done through EventPublisher.
type Repository interface {
	// Insert writes a new order row. CreatedAt is ignored: the row timestamp
	// is DB time (DEFAULT now()) so all instances agree regardless of host
	// clock skew.
	Insert(ctx context.Context, o Order) error

	// Get returns the order by id (storage error when absent).
	Get(ctx context.Context, id uuid.UUID) (Order, error)

	// MarkPaymentOutcome records a terminal payment outcome, guarded so it
	// only applies when the row is still in 'created' (the state machine's
	// only non-terminal status) — the guard makes "first outcome wins" safe
	// under concurrent/duplicate delivery. Returns false when the guard did
	// not match (row missing or already terminal).
	MarkPaymentOutcome(ctx context.Context, id uuid.UUID, to Status) (applied bool, err error)

	// MarkTimeoutEmitted is the compare-and-set claim for the unpaid
	// watcher: it flips payment_timeout_emitted and moves the status to
	// 'payment_timeout' in one guarded statement. false = another poll or
	// instance already claimed the order, or a payment landed meanwhile.
	MarkTimeoutEmitted(ctx context.Context, id uuid.UUID) (claimed bool, err error)

	// ListUnpaidExpired returns up to limit orders still 'created' (and not
	// yet timeout-claimed) older than unpaidFor. The cutoff is evaluated
	// against the DATABASE clock (now() in SQL), deliberately not an
	// injected application clock: every instance must agree on expiry
	// regardless of app-host clock skew, and the comparison column
	// (created_at) is DB time too.
	ListUnpaidExpired(ctx context.Context, unpaidFor time.Duration, limit int32) ([]UnpaidOrder, error)
}

// EventPublisher is the outbox port the Service enqueues domain events
// through; *outbox.Repository implements it directly. Atomicity with the
// order-row write is NOT provided by this interface — it comes from the
// shared ambient transaction: both the Repository and *outbox.Repository
// resolve their DBTX from the same ctx, so enqueue and row write commit (or
// roll back) together whenever the caller runs them under one transaction.
type EventPublisher interface {
	Enqueue(ctx context.Context, msg outbox.Message) error
}
