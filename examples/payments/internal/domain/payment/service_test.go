package payment_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-boilerplate/examples/payments/internal/domain/payment"
	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// fixedClock is a deterministic clock.Clock for the decision rule's
// occurred_at timestamp.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// fakeRepo captures inserted payments.
type fakeRepo struct {
	inserted []payment.Payment
	err      error
}

func (f *fakeRepo) Insert(_ context.Context, p payment.Payment) error {
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, p)
	return nil
}

func (f *fakeRepo) GetByOrder(context.Context, string) (payment.Payment, error) {
	return payment.Payment{}, errors.New("not implemented")
}

// fakePublisher captures enqueued outbox messages.
type fakePublisher struct {
	msgs []outbox.Message
}

func (f *fakePublisher) Enqueue(_ context.Context, msg outbox.Message) error {
	f.msgs = append(f.msgs, msg)
	return nil
}

const outTopic = "payments.events"

func newService(repo *fakeRepo, pub *fakePublisher, now time.Time) *payment.Service {
	return payment.NewService(repo, pub, fixedClock{now: now}, outTopic)
}

// TestService_Process_ThresholdRule pins the demo decline rule that drives
// the choreography failure path: amounts at or above DeclineThresholdCents
// are declined (status "failed" + PaymentFailed event), everything below is
// processed (PaymentProcessed). The failure event's occurred_at comes from
// the INJECTED clock — deterministic here, not wall-clock.
func TestService_Process_ThresholdRule(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 10, 9, 30, 0, 0, time.UTC)

	tests := []struct {
		name        string
		amountCents int64
		wantStatus  string
	}{
		{"below threshold is processed", payment.DeclineThresholdCents - 1, payment.StatusProcessed},
		{"at threshold is declined", payment.DeclineThresholdCents, payment.StatusFailed},
		{"above threshold is declined", payment.DeclineThresholdCents + 1, payment.StatusFailed},
		{"small amount is processed", 1, payment.StatusProcessed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeRepo{}
			pub := &fakePublisher{}
			svc := newService(repo, pub, now)

			res, err := svc.Process(context.Background(), payment.ProcessParams{
				OrderID: "order-1", AmountCents: tt.amountCents, Currency: "USD",
			})
			require.NoError(t, err, "a decline is a valid domain outcome, not a handler error")
			assert.Equal(t, tt.wantStatus, res.Status)
			require.NotEmpty(t, res.PaymentID)

			// The payment row is persisted with the decided status.
			require.Len(t, repo.inserted, 1)
			row := repo.inserted[0]
			assert.Equal(t, res.PaymentID, row.ID.String())
			assert.Equal(t, "order-1", row.OrderID)
			assert.Equal(t, tt.amountCents, row.AmountCents)
			assert.Equal(t, tt.wantStatus, row.Status)

			// Exactly one event, on the configured topic, keyed by order.
			require.Len(t, pub.msgs, 1)
			msg := pub.msgs[0]
			assert.Equal(t, outTopic, msg.Topic)
			assert.Equal(t, "payment", msg.AggregateType)
			assert.Equal(t, "order-1", msg.AggregateID)

			switch tt.wantStatus {
			case payment.StatusProcessed:
				assert.Equal(t, payment.PaymentProcessedEventType, msg.EventType)
				var evt ordersv1.PaymentProcessed
				require.NoError(t, proto.Unmarshal(msg.Payload, &evt))
				assert.Equal(t, "order-1", evt.GetOrderId())
				assert.Equal(t, res.PaymentID, evt.GetPaymentId())
				assert.Equal(t, "processed", evt.GetStatus())
			case payment.StatusFailed:
				assert.Equal(t, payment.PaymentFailedEventType, msg.EventType)
				var evt ordersv1.PaymentFailed
				require.NoError(t, proto.Unmarshal(msg.Payload, &evt))
				assert.Equal(t, "order-1", evt.GetOrderId())
				assert.Equal(t, "declined", evt.GetReason())
				require.NotNil(t, evt.GetOccurredAt(), "failure event carries its occurrence time")
				assert.Equal(t, now, evt.GetOccurredAt().AsTime(),
					"occurred_at must come from the injected clock, not time.Now")
			}
		})
	}
}

// TestService_Process_RepoFailure pins that a failed row write propagates and
// enqueues nothing (atomicity shape: no event without the row).
func TestService_Process_RepoFailure(t *testing.T) {
	t.Parallel()
	boom := errors.New("insert blew up")
	repo := &fakeRepo{err: boom}
	pub := &fakePublisher{}
	svc := newService(repo, pub, time.Now().UTC())

	_, err := svc.Process(context.Background(), payment.ProcessParams{
		OrderID: "order-1", AmountCents: 100, Currency: "USD",
	})
	require.ErrorIs(t, err, boom)
	assert.Empty(t, pub.msgs)
}

// TestService_Process_UniquePaymentIDs pins that every processed payment gets
// its own id (the id is generated per call, not per service).
func TestService_Process_UniquePaymentIDs(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	svc := newService(repo, &fakePublisher{}, time.Now().UTC())

	res1, err := svc.Process(context.Background(), payment.ProcessParams{OrderID: "o1", AmountCents: 1, Currency: "USD"})
	require.NoError(t, err)
	res2, err := svc.Process(context.Background(), payment.ProcessParams{OrderID: "o2", AmountCents: 1, Currency: "USD"})
	require.NoError(t, err)
	assert.NotEqual(t, res1.PaymentID, res2.PaymentID)
	_, err = uuid.Parse(res1.PaymentID)
	assert.NoError(t, err)
}
