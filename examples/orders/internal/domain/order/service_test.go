package order_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// fakeRepo is a function-field fake of order.Repository — service unit tests
// stay Docker-free.
type fakeRepo struct {
	insert             func(ctx context.Context, o order.Order) error
	markPaymentOutcome func(ctx context.Context, id uuid.UUID, to order.Status) (bool, error)
	markTimeout        func(ctx context.Context, id uuid.UUID) (bool, error)
	listUnpaid         func(ctx context.Context, unpaidFor time.Duration, limit int32) ([]order.UnpaidOrder, error)
}

func (f *fakeRepo) Insert(ctx context.Context, o order.Order) error { return f.insert(ctx, o) }

func (f *fakeRepo) Get(context.Context, uuid.UUID) (order.Order, error) {
	return order.Order{}, errors.New("not implemented")
}

func (f *fakeRepo) MarkPaymentOutcome(ctx context.Context, id uuid.UUID, to order.Status) (bool, error) {
	return f.markPaymentOutcome(ctx, id, to)
}

func (f *fakeRepo) MarkTimeoutEmitted(ctx context.Context, id uuid.UUID) (bool, error) {
	return f.markTimeout(ctx, id)
}

func (f *fakeRepo) ListUnpaidExpired(ctx context.Context, unpaidFor time.Duration, limit int32) ([]order.UnpaidOrder, error) {
	return f.listUnpaid(ctx, unpaidFor, limit)
}

// fakePublisher captures enqueued outbox messages.
type fakePublisher struct {
	msgs []outbox.Message
	err  error
}

func (f *fakePublisher) Enqueue(_ context.Context, msg outbox.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msg)
	return nil
}

func TestService_Create(t *testing.T) {
	t.Parallel()

	t.Run("inserts row and enqueues OrderCreated atomically-shaped", func(t *testing.T) {
		t.Parallel()
		var inserted []order.Order
		repo := &fakeRepo{insert: func(_ context.Context, o order.Order) error {
			inserted = append(inserted, o)
			return nil
		}}
		pub := &fakePublisher{}
		svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), 15*time.Minute)

		id := uuid.NewString()
		require.NoError(t, svc.Create(context.Background(), order.CreateParams{
			OrderID: id, CustomerID: "cust-1", AmountCents: 1500, Currency: "USD",
		}))

		require.Len(t, inserted, 1)
		assert.Equal(t, uuid.MustParse(id), inserted[0].ID)
		assert.Equal(t, order.StatusCreated, inserted[0].Status, "a new order always starts in 'created'")
		assert.Equal(t, "cust-1", inserted[0].CustomerID)
		assert.EqualValues(t, 1500, inserted[0].AmountCents)
		assert.Equal(t, "USD", inserted[0].Currency)

		require.Len(t, pub.msgs, 1)
		msg := pub.msgs[0]
		assert.Equal(t, order.EventsTopic, msg.Topic)
		assert.Equal(t, "order", msg.AggregateType)
		assert.Equal(t, id, msg.AggregateID)
		assert.Equal(t, order.OrderCreatedEventType, msg.EventType)
		var evt ordersv1.OrderCreated
		require.NoError(t, proto.Unmarshal(msg.Payload, &evt))
		assert.Equal(t, id, evt.GetOrderId())
		assert.Equal(t, "cust-1", evt.GetCustomerId())
		assert.EqualValues(t, 1500, evt.GetAmountCents())
		assert.Equal(t, "USD", evt.GetCurrency())
	})

	t.Run("rejects non-UUID order id before touching repo or outbox", func(t *testing.T) {
		t.Parallel()
		// nil deps prove nothing is touched on the rejection path.
		svc := order.NewService(nil, nil, slog.New(slog.DiscardHandler), 15*time.Minute)

		for _, bad := range []string{"", "not-a-uuid", "12345", "ffffffff-ffff-ffff-ffff-fffffffffffg"} {
			err := svc.Create(context.Background(), order.CreateParams{
				OrderID: bad, CustomerID: "c", AmountCents: 1, Currency: "USD",
			})
			require.Error(t, err, "order id %q must be rejected", bad)
			assert.Equal(t, order.CodeInvalidOrderID, apperr.Code(err))
			assert.True(t, apperr.IsPermanent(err), "malformed input must short-circuit to the DLT, not retry")
			assert.ErrorContains(t, err, "invalid order id")
		}
	})

	t.Run("repo failure propagates and nothing is enqueued", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("insert blew up")
		repo := &fakeRepo{insert: func(context.Context, order.Order) error { return boom }}
		pub := &fakePublisher{}
		svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), 15*time.Minute)

		err := svc.Create(context.Background(), order.CreateParams{
			OrderID: uuid.NewString(), CustomerID: "c", AmountCents: 1, Currency: "USD",
		})
		require.ErrorIs(t, err, boom)
		assert.Empty(t, pub.msgs, "no event may be enqueued when the row write failed")
	})
}

func TestService_ApplyPaymentOutcome(t *testing.T) {
	t.Parallel()

	t.Run("records terminal outcome from created", func(t *testing.T) {
		t.Parallel()
		for _, outcome := range []order.Status{order.StatusPaid, order.StatusPaymentFailed} {
			var gotID uuid.UUID
			var gotStatus order.Status
			repo := &fakeRepo{markPaymentOutcome: func(_ context.Context, id uuid.UUID, to order.Status) (bool, error) {
				gotID, gotStatus = id, to
				return true, nil
			}}
			svc := order.NewService(repo, &fakePublisher{}, slog.New(slog.DiscardHandler), time.Minute)

			id := uuid.New()
			require.NoError(t, svc.ApplyPaymentOutcome(context.Background(), id.String(), outcome))
			assert.Equal(t, id, gotID)
			assert.Equal(t, outcome, gotStatus)
		}
	})

	t.Run("first outcome wins: ignored duplicate logs the compensation warn", func(t *testing.T) {
		t.Parallel()
		repo := &fakeRepo{markPaymentOutcome: func(context.Context, uuid.UUID, order.Status) (bool, error) {
			return false, nil // row missing or already terminal
		}}
		var buf bytes.Buffer
		svc := order.NewService(repo, &fakePublisher{},
			slog.New(slog.NewTextHandler(&buf, nil)), time.Minute)

		err := svc.ApplyPaymentOutcome(context.Background(), uuid.NewString(), order.StatusPaid)
		require.NoError(t, err, "an ignored outcome is not an error — erroring would loop redelivery forever")
		assert.Contains(t, buf.String(), "level=WARN")
		assert.Contains(t, buf.String(), "compensation",
			"the warn line is the operational signal that a charge on a timed-out order needs compensation (ADR-0007)")
	})

	t.Run("state machine rejects non-outcome targets", func(t *testing.T) {
		t.Parallel()
		// nil repo proves the state machine is consulted BEFORE any write.
		svc := order.NewService(nil, nil, slog.New(slog.DiscardHandler), time.Minute)

		err := svc.ApplyPaymentOutcome(context.Background(), uuid.NewString(), order.StatusCreated)
		require.Error(t, err)
		assert.Equal(t, order.CodeInvalidStatusTransition, apperr.Code(err))
		assert.True(t, apperr.IsPermanent(err))
	})

	t.Run("rejects non-UUID order id as permanent", func(t *testing.T) {
		t.Parallel()
		svc := order.NewService(nil, nil, slog.New(slog.DiscardHandler), time.Minute)

		err := svc.ApplyPaymentOutcome(context.Background(), "not-a-uuid", order.StatusPaid)
		require.Error(t, err)
		assert.Equal(t, order.CodeInvalidOrderID, apperr.Code(err))
		assert.True(t, apperr.IsPermanent(err))
	})
}

func TestService_EmitPaymentTimeout(t *testing.T) {
	t.Parallel()

	t.Run("claimed order enqueues exactly one timeout event", func(t *testing.T) {
		t.Parallel()
		var marked []uuid.UUID
		repo := &fakeRepo{markTimeout: func(_ context.Context, id uuid.UUID) (bool, error) {
			marked = append(marked, id)
			return true, nil
		}}
		pub := &fakePublisher{}
		deadline := 15 * time.Minute
		svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), deadline)

		id := uuid.New()
		createdAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
		require.NoError(t, svc.EmitPaymentTimeout(context.Background(), id, createdAt))

		assert.Equal(t, []uuid.UUID{id}, marked)
		require.Len(t, pub.msgs, 1)
		msg := pub.msgs[0]
		assert.Equal(t, order.EventsTopic, msg.Topic)
		assert.Equal(t, "order", msg.AggregateType)
		assert.Equal(t, id.String(), msg.AggregateID)
		assert.Equal(t, order.OrderPaymentTimedOutEventType, msg.EventType)
		var evt ordersv1.OrderPaymentTimedOut
		require.NoError(t, proto.Unmarshal(msg.Payload, &evt))
		assert.Equal(t, id.String(), evt.GetOrderId())
		assert.Equal(t, createdAt.Add(deadline), evt.GetDeadline().AsTime(),
			"event deadline is created_at + payment window, not wall-clock now")
	})

	t.Run("lost CAS claim enqueues nothing", func(t *testing.T) {
		t.Parallel()
		repo := &fakeRepo{markTimeout: func(context.Context, uuid.UUID) (bool, error) {
			return false, nil // another poll/instance won, or a payment landed
		}}
		pub := &fakePublisher{}
		svc := order.NewService(repo, pub, slog.New(slog.DiscardHandler), time.Minute)

		require.NoError(t, svc.EmitPaymentTimeout(context.Background(), uuid.New(), time.Now()))
		assert.Empty(t, pub.msgs, "losing the compare-and-set guard must suppress the event — that is the exactly-once mechanism")
	})
}

func TestService_ListUnpaidExpired(t *testing.T) {
	t.Parallel()

	want := []order.UnpaidOrder{{ID: uuid.New(), CreatedAt: time.Now().UTC()}}
	var gotUnpaidFor time.Duration
	var gotLimit int32
	repo := &fakeRepo{listUnpaid: func(_ context.Context, unpaidFor time.Duration, limit int32) ([]order.UnpaidOrder, error) {
		gotUnpaidFor, gotLimit = unpaidFor, limit
		return want, nil
	}}
	svc := order.NewService(repo, &fakePublisher{}, slog.New(slog.DiscardHandler), 9*time.Minute)

	got, err := svc.ListUnpaidExpired(context.Background(), 42)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 9*time.Minute, gotUnpaidFor, "the service owns the payment window; callers only choose batch size")
	assert.EqualValues(t, 42, gotLimit)
}
