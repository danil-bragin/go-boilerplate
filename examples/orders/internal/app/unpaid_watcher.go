package app

import (
	"context"
	"fmt"

	"go-boilerplate/examples/orders/internal/domain/order"
	"go-boilerplate/platform/storage/pg"
)

// unpaidBatchLimit bounds how many expired orders one poll processes; the
// loop drains until a short batch, so a backlog clears within one tick.
const unpaidBatchLimit = 100

// UnpaidWatcher periodically scans for orders that are still 'created' past
// the payment deadline and emits an OrderPaymentTimedOut event (via the
// outbox) exactly once per order.
//
// LAYERING — the watcher is a thin adapter: it owns the POLL LOOP and the
// TRANSACTION BOUNDARY (each candidate is claimed inside its own pg.RunInTx
// so the compare-and-set and the outbox enqueue commit together) and
// delegates all business decisions — the expiry window, the CAS claim, the
// event payload — to order.Service.
//
// DESIGN — why the watcher is a pure local query: the orders service tracks
// payment outcomes on its OWN order rows by consuming payments.events
// (PaymentProcessed → status 'paid', PaymentFailed → 'payment_failed'; see
// transport.NewPaymentsEventHandler). That keeps the watcher query local —
// no cross-database peeking into the payments service — and the inbox-deduped
// consumer guarantees the status transition happens at most once. An order
// that was paid or failed before the deadline therefore never matches the
// watcher's WHERE status='created' filter.
//
// Exactly-once emission lives in order.Service.EmitPaymentTimeout: re-polls,
// restarts, and concurrent instances all collapse on the guarded UPDATE —
// a lost claim means someone else won and no event is enqueued.
type UnpaidWatcher struct {
	pool *pg.Pool
	svc  *order.Service
}

// NewUnpaidWatcher builds a watcher over the domain service. The payment
// window is owned by the service (order.NewService); the poll cadence is
// owned by the caller: register Poll via servicekit's
// Service.AddPeriodicWorker with ORDERS_UNPAID_CHECK_INTERVAL as the
// interval.
func NewUnpaidWatcher(pool *pg.Pool, svc *order.Service) *UnpaidWatcher {
	return &UnpaidWatcher{pool: pool, svc: svc}
}

// Poll drains all currently-expired unpaid orders in batches. It is the
// per-tick body for servicekit's AddPeriodicWorker — typically registered
// singleActive so only one instance scans, though the compare-and-set guard
// in the service keeps concurrent polls exactly-once anyway.
func (w *UnpaidWatcher) Poll(ctx context.Context) error {
	for {
		rows, err := w.svc.ListUnpaidExpired(ctx, unpaidBatchLimit)
		if err != nil {
			return fmt.Errorf("unpaid_watcher: %w", err)
		}
		for _, row := range rows {
			// One transaction per order: EmitPaymentTimeout requires a
			// caller-owned tx so its CAS claim and outbox enqueue commit
			// atomically (see its godoc).
			if err := pg.RunInTx(ctx, w.pool, func(ctx context.Context) error {
				return w.svc.EmitPaymentTimeout(ctx, row.ID, row.CreatedAt)
			}); err != nil {
				return fmt.Errorf("unpaid_watcher: %w", err)
			}
		}
		if len(rows) < unpaidBatchLimit {
			return nil
		}
	}
}
