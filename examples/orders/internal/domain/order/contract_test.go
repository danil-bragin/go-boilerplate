package order_test

// Producer half of the cross-service contract tests (-short, no Docker):
// the REAL order.Service runs against capturing fakes and its enqueued
// outbox.Message is asserted equal to the canonical fixture in
// examples/e2e/contract. The consumer halves (gateway projection, payments
// transport) drive the same fixture through the real wire mapping — see the
// contract package doc for how the two halves pin semantic drift together.

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go-boilerplate/examples/e2e/contract"
	"go-boilerplate/examples/orders/internal/domain/order"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestContract_OrderCreated pins what the real Service.Create puts on the
// wire: topic, aggregate (key = order id), versioned event type, and every
// payload field (a field that stops being populated fails proto-equality).
func TestContract_OrderCreated(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{insert: func(context.Context, order.Order) error { return nil }}
	pub := &fakePublisher{}
	svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), 15*time.Minute)

	orderID := uuid.NewString()
	require.NoError(t, svc.Create(context.Background(), order.CreateParams{
		OrderID:    orderID,
		CustomerID: "cust-42",
		Amount:     amt(123_456, "EUR"),
	}))

	require.Len(t, pub.msgs, 1, "Create must enqueue exactly one event")
	contract.RequireSameMessage[*ordersv1.OrderCreated](t,
		contract.OrderCreated(t, orderID, "cust-42", 123_456, "EUR"), pub.msgs[0])
}

// TestContract_OrderPaymentTimedOut pins the timeout event: the deadline
// must derive from the row's created_at + the payment window (stable across
// re-emission), never wall-clock now.
func TestContract_OrderPaymentTimedOut(t *testing.T) {
	t.Parallel()
	const window = 9 * time.Minute
	repo := &fakeRepo{markTimeout: func(context.Context, uuid.UUID) (bool, error) { return true, nil }}
	pub := &fakePublisher{}
	svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), window)

	orderID := uuid.New()
	createdAt := time.Date(2026, 6, 11, 8, 0, 0, 0, time.UTC)
	require.NoError(t, svc.EmitPaymentTimeout(context.Background(), orderID, createdAt))

	require.Len(t, pub.msgs, 1, "a won claim must enqueue exactly one event")
	contract.RequireSameMessage[*ordersv1.OrderPaymentTimedOut](t,
		contract.OrderPaymentTimedOut(t, orderID.String(), createdAt.Add(window)), pub.msgs[0])
}
