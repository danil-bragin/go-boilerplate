package payment

import (
	"context"
	"fmt"
	"math/big"

	"go-boilerplate/platform/clock"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/money"

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

// DeclineThresholdMinor is the deterministic demo decline rule, expressed in
// the asset's smallest unit: payments whose amount is at or above this many
// minor units are declined (10 000.00 for a 2-decimal fiat). It gives the
// choreography a reproducible failure path (PaymentFailed) without external
// dependencies. Building the threshold in the payment's own asset keeps the
// comparison same-asset (and so generalizes beyond 2-decimal fiat).
const DeclineThresholdMinor = 1_000_000

// ProcessParams are the validated inputs to Service.Process. Amount is a
// precision-exact money value; the cents+currency from the wire are converted
// to it at the entry point (see internal/app).
type ProcessParams struct {
	OrderID string
	Amount  money.Money
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
	declined, err := isDeclined(p.Amount)
	if err != nil {
		return Result{}, fmt.Errorf("payment: decide %s: %w", p.OrderID, err)
	}
	status, eventType, event := s.outcome(p, paymentID, declined)

	if err := s.repo.Insert(ctx, Payment{
		ID:      paymentID,
		OrderID: p.OrderID,
		Amount:  p.Amount,
		Status:  status,
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

// isDeclined applies the demo decline rule: the amount is compared (same-asset)
// against the threshold built in the amount's own asset. An error only arises
// if the asset is somehow unregistered (it cannot be, since a money.Money is
// always constructed with a registered asset).
func isDeclined(amount money.Money) (bool, error) {
	threshold, err := money.FromMinor(big.NewInt(DeclineThresholdMinor), amount.Asset())
	if err != nil {
		return false, fmt.Errorf("threshold for %s: %w", amount.Asset(), err)
	}
	return amount.GreaterThanOrEqual(threshold)
}

// outcome maps the decision to the resulting payment status, versioned event
// type, and event payload. Declined payments emit PaymentFailed (the
// choreography failure branch) with the injected clock's now as occurred_at;
// everything else is processed.
func (s *Service) outcome(p ProcessParams, paymentID uuid.UUID, declined bool) (status, eventType string, event proto.Message) {
	if declined {
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
