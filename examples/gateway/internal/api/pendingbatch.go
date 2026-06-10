package api

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go-boilerplate/platform/storage/pg"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// Pending-batcher defaults. ≤50 ms of added GET-after-POST staleness and
// ≤100-row multi-row INSERTs keep both latency and statement size bounded.
const (
	defaultPendingFlushInterval = 50 * time.Millisecond
	defaultPendingMaxBatch      = 100
	defaultPendingBufferSize    = 1024
	// pendingDrainTimeout bounds the final flush during shutdown — the
	// worker ctx is already cancelled, so the drain runs on a fresh deadline.
	pendingDrainTimeout = 5 * time.Second
)

// PendingBatcherOption tunes a PendingBatcher (tests mostly).
type PendingBatcherOption func(*PendingBatcher)

// WithPendingFlushInterval overrides the flush tick (default 50ms).
func WithPendingFlushInterval(d time.Duration) PendingBatcherOption {
	return func(b *PendingBatcher) {
		if d > 0 {
			b.flushInterval = d
		}
	}
}

// WithPendingMaxBatch overrides the row count that forces a flush (default 100).
func WithPendingMaxBatch(n int) PendingBatcherOption {
	return func(b *PendingBatcher) {
		if n > 0 {
			b.maxBatch = n
		}
	}
}

// WithPendingBufferSize overrides the enqueue buffer capacity (default 1024).
func WithPendingBufferSize(n int) PendingBatcherOption {
	return func(b *PendingBatcher) {
		if n > 0 {
			b.bufSize = n
		}
	}
}

// PendingBatcher is the GATEWAY_PENDING_ASYNC=true write path for the
// POST-time "pending" read-model row: instead of one synchronous INSERT per
// request on the writer pool, CreateOrder enqueues the row here and a single
// goroutine (Run, wired as a servicekit worker) flushes the buffer as ONE
// multi-row INSERT ... ON CONFLICT (order_id) DO NOTHING every ≤50 ms or
// every ≤100 rows, whichever comes first.
//
// Loss model: the pending row is best-effort BY DESIGN (the sync path already
// tolerates a failed insert — the projection creates the row when
// OrderCreated arrives). Accordingly:
//   - a full buffer DROPS the row (WARN + gateway.pending_async.dropped
//     counter) rather than back-pressuring the request;
//   - a failed flush WARNs and drops the batch (same as a failed sync insert);
//   - shutdown drains: Run flushes everything buffered before returning, on a
//     fresh bounded context (servicekit cancels workers before closing pg).
//
// Client-visible trade-off: GET immediately after POST may 404 until the
// batch flushes; clients retry / SSE snapshot covers (documented on
// GATEWAY_PENDING_ASYNC).
type PendingBatcher struct {
	pool          *pg.Pool
	logger        *slog.Logger
	buf           chan storegen.InsertPendingOrderParams
	dropped       metric.Int64Counter
	flushInterval time.Duration
	maxBatch      int
	bufSize       int
}

// NewPendingBatcher builds a batcher. Wire Run via servicekit's AddWorker so
// it starts with the service and drains on shutdown.
func NewPendingBatcher(pool *pg.Pool, logger *slog.Logger, opts ...PendingBatcherOption) *PendingBatcher {
	b := &PendingBatcher{
		pool:          pool,
		logger:        logger,
		flushInterval: defaultPendingFlushInterval,
		maxBatch:      defaultPendingMaxBatch,
		bufSize:       defaultPendingBufferSize,
	}
	for _, o := range opts {
		o(b)
	}
	b.buf = make(chan storegen.InsertPendingOrderParams, b.bufSize)
	// Global-meter instrument, mirroring platform metrics helpers: a failed
	// creation degrades to nil (no-op at the call site) — metrics must never
	// break the write path.
	if c, err := otel.Meter("gateway.api").Int64Counter(
		"gateway.pending_async.dropped",
		metric.WithDescription("Best-effort pending read-model rows dropped because the async buffer was full"),
	); err == nil {
		b.dropped = c
	}
	return b
}

// Enqueue buffers one pending row without blocking. When the buffer is full
// the row is dropped (best-effort contract): WARN + dropped counter.
func (b *PendingBatcher) Enqueue(ctx context.Context, p storegen.InsertPendingOrderParams) {
	select {
	case b.buf <- p:
	default:
		if b.dropped != nil {
			b.dropped.Add(ctx, 1)
		}
		b.logger.WarnContext(ctx, "gateway: pending-row buffer full — dropping best-effort insert (projection will create the row on OrderCreated)",
			"order_id", p.OrderID.String())
	}
}

// Run is the single flusher goroutine. It returns only after ctx is cancelled
// AND the remaining buffer has been flushed (graceful drain).
func (b *PendingBatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]storegen.InsertPendingOrderParams, 0, b.maxBatch)
	for {
		select {
		case <-ctx.Done():
			b.drain(batch)
			return
		case p := <-b.buf:
			batch = append(batch, p)
			if len(batch) >= b.maxBatch {
				// During shutdown select picks randomly between the
				// cancelled ctx and a ready buffer: flushing on the dead
				// worker ctx would fail instantly and drop the whole batch.
				// Divert to the drain path (fresh bounded ctx) instead.
				if ctx.Err() != nil {
					b.drain(batch)
					return
				}
				b.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if ctx.Err() != nil {
					b.drain(batch)
					return
				}
				b.flush(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

// drain empties the buffer and flushes everything on a fresh bounded context
// (the worker ctx is already cancelled; the pg pool is still open — servicekit
// closes pools only after workers have returned).
func (b *PendingBatcher) drain(batch []storegen.InsertPendingOrderParams) {
	drainCtx, cancel := context.WithTimeout(context.Background(), pendingDrainTimeout)
	defer cancel()
	for {
		select {
		case p := <-b.buf:
			batch = append(batch, p)
			if len(batch) >= b.maxBatch {
				b.flush(drainCtx, batch)
				batch = batch[:0]
			}
		default:
			b.flush(drainCtx, batch)
			return
		}
	}
}

// flush writes the batch as one multi-row INSERT ... ON CONFLICT (order_id)
// DO NOTHING. DO NOTHING (unlike DO UPDATE) tolerates duplicate order ids
// WITHIN one statement — an idempotent POST retry landing twice in the same
// batch is skipped, not an error — and never downgrades a row a racing
// projection write already upgraded. A failed flush is logged and dropped
// (best-effort, same contract as the sync path's failed insert).
func (b *PendingBatcher) flush(ctx context.Context, batch []storegen.InsertPendingOrderParams) {
	if len(batch) == 0 {
		return
	}
	// Hand-built multi-row INSERT (sqlc has no multirow variant for this
	// query). MUST stay column-compatible with InsertPendingOrder in
	// internal/store/queries/gateway.sql — change both together.
	var sb strings.Builder
	sb.WriteString("insert into orders_read (order_id, customer_id, amount_cents, currency, status, updated_at) values ")
	args := make([]any, 0, len(batch)*4)
	for i, p := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 4
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,'pending',now())", n+1, n+2, n+3, n+4)
		args = append(args, p.OrderID, p.CustomerID, p.AmountCents, p.Currency)
	}
	sb.WriteString(" on conflict (order_id) do nothing")

	if _, err := b.pool.Writer().Exec(ctx, sb.String(), args...); err != nil {
		// Failed flushes are the likeliest loss mode — count them in the same
		// dropped metric operators are told to watch, not just buffer-fulls.
		if b.dropped != nil {
			b.dropped.Add(ctx, int64(len(batch)))
		}
		b.logger.WarnContext(ctx, "gateway: async pending-row batch insert failed (rows dropped — best-effort)",
			"rows", len(batch), "error", err)
	}
}
