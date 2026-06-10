package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go-boilerplate/examples/orders/internal/store/gen"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// OrderPaymentTimedOutEventType is the versioned event type emitted on
// orders.events when an order stays unpaid past the payment deadline
// ("orders.OrderPaymentTimedOut.v1", derived from the proto message).
var OrderPaymentTimedOutEventType = consume.EventTypeFor[*ordersv1.OrderPaymentTimedOut](1)

// unpaidBatchLimit bounds how many expired orders one poll processes; the
// loop drains until a short batch, so a backlog clears within one tick.
const unpaidBatchLimit = 100

// UnpaidWatcher periodically scans for orders that are still 'created' past
// the payment deadline and emits an OrderPaymentTimedOut event (via the
// outbox) exactly once per order.
//
// DESIGN — why the watcher is a pure local query: the orders service now
// tracks payment outcomes on its OWN order rows by consuming payments.events
// (PaymentProcessed → status 'paid', PaymentFailed → 'payment_failed'; see
// transport.NewPaymentsEventHandler). That keeps the watcher query local —
// no cross-database peeking into the payments service — and the inbox-deduped
// consumer guarantees the status transition happens at most once. An order
// that was paid or failed before the deadline therefore never matches the
// watcher's WHERE status='created' filter.
//
// Exactly-once emission: payment_timeout_emitted is flipped with a guarded
// UPDATE (compare-and-set) in the SAME transaction that enqueues the outbox
// row. Re-polls, restarts, and concurrent instances all collapse on the
// guard — 0 rows updated means someone else won and no event is enqueued.
type UnpaidWatcher struct {
	pool       *pg.Pool
	outboxRepo *outbox.Repository
	deadline   time.Duration
	logger     *slog.Logger
}

// NewUnpaidWatcher builds a watcher. deadline is the payment window
// (ORDERS_PAYMENT_DEADLINE). The poll cadence is owned by the caller:
// register Poll via servicekit's Service.AddPeriodicWorker with
// ORDERS_UNPAID_CHECK_INTERVAL as the interval.
func NewUnpaidWatcher(
	pool *pg.Pool,
	outboxRepo *outbox.Repository,
	deadline time.Duration,
	logger *slog.Logger,
) *UnpaidWatcher {
	return &UnpaidWatcher{
		pool:       pool,
		outboxRepo: outboxRepo,
		deadline:   deadline,
		logger:     logger,
	}
}

// Poll drains all currently-expired unpaid orders in batches. It is the
// per-tick body for servicekit's AddPeriodicWorker — typically registered
// singleActive so only one instance scans, though the compare-and-set guard
// in emitTimeout keeps concurrent polls exactly-once anyway.
func (w *UnpaidWatcher) Poll(ctx context.Context) error {
	for {
		rows, err := gen.New(w.pool.Writer()).ListUnpaidExpired(ctx, gen.ListUnpaidExpiredParams{
			DeadlineSeconds: w.deadline.Seconds(),
			BatchLimit:      unpaidBatchLimit,
		})
		if err != nil {
			return fmt.Errorf("unpaid_watcher: list expired: %w", err)
		}
		for _, row := range rows {
			if err := w.emitTimeout(ctx, row.ID, row.CreatedAt.Time); err != nil {
				return err
			}
		}
		if len(rows) < unpaidBatchLimit {
			return nil
		}
	}
}

// emitTimeout marks the order and enqueues the event atomically.
func (w *UnpaidWatcher) emitTimeout(ctx context.Context, orderID uuid.UUID, createdAt time.Time) error {
	return pg.RunInTx(ctx, w.pool, func(ctx context.Context) error {
		// Compare-and-set: claims the order. 0 rows = already claimed by a
		// concurrent poll/instance, or a payment landed since the SELECT.
		claimed, err := gen.New(pg.FromContext(ctx, w.pool)).MarkPaymentTimeoutEmitted(ctx, orderID)
		if err != nil {
			return fmt.Errorf("unpaid_watcher: mark emitted %s: %w", orderID, err)
		}
		if claimed == 0 {
			return nil
		}

		evt := &ordersv1.OrderPaymentTimedOut{
			OrderId:  orderID.String(),
			Deadline: timestamppb.New(createdAt.Add(w.deadline)),
		}
		payload, err := proto.Marshal(evt)
		if err != nil {
			return fmt.Errorf("unpaid_watcher: marshal event: %w", err)
		}
		if err := w.outboxRepo.Enqueue(ctx, outbox.Message{
			ID:            uuid.New(),
			Topic:         "orders.events",
			AggregateType: "order",
			AggregateID:   orderID.String(),
			EventType:     OrderPaymentTimedOutEventType,
			Payload:       payload,
		}); err != nil {
			return fmt.Errorf("unpaid_watcher: enqueue event: %w", err)
		}
		w.logger.Info("unpaid watcher: payment deadline exceeded, OrderPaymentTimedOut enqueued",
			"order_id", orderID, "deadline", w.deadline)
		return nil
	})
}
