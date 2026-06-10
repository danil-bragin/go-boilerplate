package payment

import (
	"context"
	"fmt"

	"go-boilerplate/platform/clock"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// Versioned event types emitted by the payments service on its events topic
// (derived from the proto messages via consume.EventTypeFor).
var (
	PaymentProcessedEventType = consume.EventTypeFor[*ordersv1.PaymentProcessed](1)
	PaymentFailedEventType    = consume.EventTypeFor[*ordersv1.PaymentFailed](1)
)

// Payment row statuses. A decline is a valid domain outcome, not an error —
// errors trigger redelivery, and a deterministic decline would just fail
// again.
const (
	// StatusProcessed marks an accepted payment (PaymentProcessed event).
	StatusProcessed = "processed"
	// StatusFailed marks a declined payment (PaymentFailed event).
	StatusFailed = "failed"
)

// DeclineThresholdCents is the deterministic demo decline rule: payments with
// amount_cents >= this threshold are declined. It gives the choreography a
// reproducible failure path (PaymentFailed) without external dependencies —
// any order of 10 000.00 currency units or more fails payment.
const DeclineThresholdCents = 1_000_000

// ProcessParams are the validated inputs to Service.Process (struct-tag
// validation happens at the entry point via the cqrs Validation behavior).
type ProcessParams struct {
	OrderID     string
	AmountCents int64
	Currency    string
}

// Result is the outcome of Service.Process: the new payment's id and its
// status (StatusProcessed or StatusFailed).
type Result struct {
	PaymentID string
	Status    string
}

// Service owns the payments business flow: decide the outcome (decline
// rule), persist the payment row, and enqueue the corresponding domain
// event. Entry points are thin adapters over Process.
//
// The clock is INJECTED because this is business code that reads "now" (the
// PaymentFailed occurred_at field) — see platform/clock for the rule; row
// timestamps stay DB time (DEFAULT now()).
type Service struct {
	repo     Repository
	events   EventPublisher
	clk      clock.Clock
	outTopic string
}

// NewService builds the payments domain service. outTopic is the topic
// payment events are published on (PAYMENTS_EVENTS_TOPIC).
func NewService(repo Repository, events EventPublisher, clk clock.Clock, outTopic string) *Service {
	return &Service{repo: repo, events: events, clk: clk, outTopic: outTopic}
}

// Process decides the payment outcome, writes the payment row, and enqueues
// the PaymentProcessed/PaymentFailed event. Run it under a transaction (the
// inbox.ProcessOnce ambient tx in production) so row and event commit
// atomically — both writes resolve their DBTX from ctx.
func (s *Service) Process(ctx context.Context, p ProcessParams) (Result, error) {
	paymentID := uuid.New()
	status, eventType, event := s.decide(p, paymentID)

	if err := s.repo.Insert(ctx, Payment{
		ID:          paymentID,
		OrderID:     p.OrderID,
		AmountCents: p.AmountCents,
		Status:      status,
	}); err != nil {
		return Result{}, fmt.Errorf("payment: insert: %w", err)
	}

	payload, err := proto.Marshal(event)
	if err != nil {
		return Result{}, fmt.Errorf("payment: marshal %s: %w", eventType, err)
	}
	if err := s.events.Enqueue(ctx, outbox.Message{
		ID:            uuid.New(),
		Topic:         s.outTopic,
		AggregateType: "payment",
		AggregateID:   p.OrderID,
		EventType:     eventType,
		Payload:       payload,
	}); err != nil {
		return Result{}, fmt.Errorf("payment: enqueue %s: %w", eventType, err)
	}

	return Result{PaymentID: paymentID.String(), Status: status}, nil
}

// decide is the pure payment decision: it maps the params to the resulting
// payment status, versioned event type, and event payload. Amounts at or
// above DeclineThresholdCents are declined (PaymentFailed — the choreography
// failure branch); everything else is processed. The decline's occurred_at
// is the injected clock's now.
func (s *Service) decide(p ProcessParams, paymentID uuid.UUID) (status, eventType string, event proto.Message) {
	if p.AmountCents >= DeclineThresholdCents {
		return StatusFailed, PaymentFailedEventType, &ordersv1.PaymentFailed{
			OrderId:    p.OrderID,
			Reason:     "declined",
			OccurredAt: timestamppb.New(s.clk.Now()),
		}
	}
	return StatusProcessed, PaymentProcessedEventType, &ordersv1.PaymentProcessed{
		OrderId:   p.OrderID,
		PaymentId: paymentID.String(),
		Status:    StatusProcessed,
	}
}
