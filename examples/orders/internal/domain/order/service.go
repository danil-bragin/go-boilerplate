package order

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/money"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// EventsTopic is the topic order domain events are published on.
const EventsTopic = "orders.events"

// Versioned event types emitted by the orders service on EventsTopic
// (derived from the proto messages via consume.EventTypeFor).
var (
	// OrderCreatedEventType is emitted when an order row is created
	// ("orders.OrderCreated.v1").
	OrderCreatedEventType = consume.EventTypeFor[*ordersv1.OrderCreated](1)
	// OrderPaymentTimedOutEventType is emitted when an order stays unpaid
	// past the payment deadline ("orders.OrderPaymentTimedOut.v1").
	OrderPaymentTimedOutEventType = consume.EventTypeFor[*ordersv1.OrderPaymentTimedOut](1)
)

// CreateParams are the validated inputs to Service.Create. Struct-tag
// validation happens at the entry point (the cqrs Validation behavior on the
// app.CreateOrder command); the service only enforces rules that need domain
// knowledge (UUID shape, state machine).
type CreateParams struct {
	OrderID    string
	CustomerID string
	Amount     money.Money
}

// Service owns the orders business rules: order creation, the payment
// outcome transition ("first outcome wins" + compensation warn), and the
// exactly-once payment-timeout emission. Entry points (cqrs handler, Kafka
// transport, watcher loop) are thin adapters over these methods — shared
// logic lives here, never in one handler calling another.
//
// No clock is injected: orders business never reads wall-clock "now". Row
// timestamps are DB time (DEFAULT now()), the expiry cutoff is computed in
// SQL against the DB clock (see Repository.ListUnpaidExpired), and the
// timeout event's deadline derives from the row's created_at. Compare
// payments' Service, which does inject platform/clock for occurred_at.
type Service struct {
	repo            Repository
	events          EventPublisher
	logger          *slog.Logger
	paymentDeadline time.Duration
}

// NewService builds the orders domain service. paymentDeadline is the
// business payment window (ORDERS_PAYMENT_DEADLINE): how long an order may
// stay 'created' before it times out.
func NewService(repo Repository, events EventPublisher, logger *slog.Logger, paymentDeadline time.Duration) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{repo: repo, events: events, logger: logger, paymentDeadline: paymentDeadline}
}

// Create inserts the order row (status 'created') and enqueues the
// OrderCreated event. Run it under a transaction (the inbox.ProcessOnce
// ambient tx in production) so row and event commit atomically — both writes
// resolve their DBTX from ctx.
func (s *Service) Create(ctx context.Context, p CreateParams) error {
	id, err := uuid.Parse(p.OrderID)
	if err != nil {
		return apperr.Wrap(err, CodeInvalidOrderID).WithParam("order_id", p.OrderID)
	}

	if err := s.repo.Insert(ctx, Order{
		ID:         id,
		CustomerID: p.CustomerID,
		Amount:     p.Amount,
		Status:     StatusCreated,
	}); err != nil {
		return fmt.Errorf("order: insert: %w", err)
	}

	// The cross-service OrderCreated event keeps the wire shape (minor units +
	// currency, the Stripe-style contract payments consumes): re-derive it from
	// the money value.
	minor, err := p.Amount.Minor()
	if err != nil {
		return fmt.Errorf("order: amount %s to minor units: %w", p.Amount, err)
	}
	return s.enqueue(ctx, p.OrderID, OrderCreatedEventType, &ordersv1.OrderCreated{
		OrderId:     p.OrderID,
		CustomerId:  p.CustomerID,
		AmountCents: minor.Int64(),
		Currency:    p.Amount.Asset(),
	})
}

// ApplyPaymentOutcome records a terminal payment outcome (StatusPaid /
// StatusPaymentFailed) on the order. The state machine is consulted first —
// an outcome that is not reachable from 'created' is a permanent
// ORDERS_INVALID_STATUS_TRANSITION (programmer/dispatch error, straight to
// the DLT). The repository's guarded UPDATE then enforces the same machine
// concurrently: first outcome wins, and an outcome arriving for an order
// already terminal is IGNORED (nil — erroring would loop redelivery
// forever), with a warning that doubles as the compensation signal:
// 'paid' landing on a timed-out order means the customer WAS charged and the
// charge needs compensation (refund / manual review, ADR-0007).
func (s *Service) ApplyPaymentOutcome(ctx context.Context, rawOrderID string, outcome Status) error {
	id, err := uuid.Parse(rawOrderID)
	if err != nil {
		return apperr.Wrap(err, CodeInvalidOrderID).WithParam("order_id", rawOrderID)
	}
	if err := Transition(StatusCreated, outcome); err != nil {
		return err
	}

	applied, err := s.repo.MarkPaymentOutcome(ctx, id, outcome)
	if err != nil {
		return fmt.Errorf("order: mark payment outcome %s=%s: %w", id, outcome, err)
	}
	if !applied {
		s.logger.Warn("orders: payment outcome ignored (order not in 'created'; a 'paid' outcome on a timed-out order means the charge needs compensation)",
			"order_id", id, "status", outcome)
	}
	return nil
}

// ListUnpaidExpired returns up to limit orders whose payment window (the
// service's paymentDeadline) has expired without a payment outcome. Expiry
// is evaluated by the database clock — see Repository.ListUnpaidExpired.
func (s *Service) ListUnpaidExpired(ctx context.Context, limit int32) ([]UnpaidOrder, error) {
	rows, err := s.repo.ListUnpaidExpired(ctx, s.paymentDeadline, limit)
	if err != nil {
		return nil, fmt.Errorf("order: list unpaid expired: %w", err)
	}
	return rows, nil
}

// EmitPaymentTimeout claims the order via the compare-and-set guard and, if
// the claim won, enqueues the OrderPaymentTimedOut event. Losing the claim
// (another poll/instance got there first, or a payment landed) is a silent
// no-op — that collapse onto the guard is the exactly-once mechanism.
//
// MUST run inside a caller-owned transaction (the watcher wraps each call in
// pg.RunInTx): the CAS write and the outbox enqueue resolve their DBTX from
// ctx, so the claim and the event commit or roll back together. Without the
// transaction the claim could commit while the enqueue fails, losing the
// event forever.
func (s *Service) EmitPaymentTimeout(ctx context.Context, orderID uuid.UUID, createdAt time.Time) error {
	claimed, err := s.repo.MarkTimeoutEmitted(ctx, orderID)
	if err != nil {
		return fmt.Errorf("order: mark timeout emitted %s: %w", orderID, err)
	}
	if !claimed {
		return nil
	}

	if err := s.enqueue(ctx, orderID.String(), OrderPaymentTimedOutEventType, &ordersv1.OrderPaymentTimedOut{
		OrderId: orderID.String(),
		// The deadline is derived from the row's created_at + the payment
		// window — not wall-clock now — so re-emission would be stable.
		Deadline: timestamppb.New(createdAt.Add(s.paymentDeadline)),
	}); err != nil {
		return err
	}
	s.logger.Info("unpaid watcher: payment deadline exceeded, OrderPaymentTimedOut enqueued",
		"order_id", orderID, "deadline", s.paymentDeadline)
	return nil
}

// enqueue marshals a domain event and hands it to the outbox publisher.
func (s *Service) enqueue(ctx context.Context, orderID, eventType string, event proto.Message) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("order: marshal %s: %w", eventType, err)
	}
	if err := s.events.Enqueue(ctx, outbox.Message{
		ID:            uuid.New(),
		Topic:         EventsTopic,
		AggregateType: "order",
		AggregateID:   orderID,
		EventType:     eventType,
		Payload:       payload,
	}); err != nil {
		return fmt.Errorf("order: enqueue %s: %w", eventType, err)
	}
	return nil
}
