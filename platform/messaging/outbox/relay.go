package outbox

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/messaging/outbox/gen"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
)

// RelayConfig configures the polling relay.
//
// RetentionAge is the maximum age of a published outbox row before it is
// eligible for deletion. CleanupInterval controls how often Cleaner.RunCleanup
// fires. Both are used when constructing a Cleaner alongside the Relay.
//
// Defaults: RetentionAge 24h, CleanupInterval 1h.
type RelayConfig struct {
	BatchSize       int32         `env:"OUTBOX_BATCH_SIZE"        envDefault:"100"`
	PollInterval    time.Duration `env:"OUTBOX_POLL_INTERVAL"     envDefault:"1s"`
	RetentionAge    time.Duration `env:"OUTBOX_RETENTION_AGE"    envDefault:"24h"`
	CleanupInterval time.Duration `env:"OUTBOX_CLEANUP_INTERVAL" envDefault:"1h"`
}

// Relay polls unpublished outbox rows and publishes them via a Publisher,
// using a three-phase design that keeps the DB transaction open only for the
// minimum time needed, dramatically reducing lock-hold time under load.
//
// Three-phase ProcessBatch:
//
//	Phase A (short tx): SELECT ... FOR UPDATE SKIP LOCKED fetches up to
//	BatchSize unpublished rows into memory and immediately commits, releasing
//	all row locks. Lock-hold time is O(one DB round-trip), not O(N·broker RTT).
//
//	Phase B (no tx): publish all fetched messages to the transport. If the
//	Publisher also implements BatchPublisher, PublishBatch is used for a
//	single broker round-trip (O(1) latency for the batch); otherwise Publish
//	is called per-record. Publishing happens entirely outside any transaction.
//
//	Phase C (short tx): UPDATE outbox SET published_at=now() WHERE id=ANY($1)
//	marks all successfully-published rows in one statement.
//
// Delivery semantics: AT-LEAST-ONCE. Duplicates are expected and safe:
//   - Crash between B and C: rows re-fetched and re-published on next poll.
//   - Two concurrent relays: after phase A commits, a second relay can claim
//     the same rows before phase C completes → both publish → inbox deduplicates
//     by Message.ID.
//
// Multiple relay goroutines/instances are safe and beneficial: SKIP LOCKED
// ensures that within a single phase-A transaction each relay claims a
// distinct subset of rows; duplicate publishes across relays are tolerated.
//
// OnError (set via SetOnError) is called for each ProcessBatch error during
// Run. Run never stops on a batch error; it backs off and retries.
type Relay struct {
	pool    *pg.Pool
	pub     Publisher
	cfg     RelayConfig
	onError func(error)
}

// NewRelay creates a Relay.
func NewRelay(pool *pg.Pool, pub Publisher, cfg RelayConfig) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Relay{pool: pool, pub: pub, cfg: cfg}
}

// SetOnError registers a callback that is called for each ProcessBatch error
// encountered during Run. It is safe to call before Run is started. Run never
// stops on a batch error — it backs off and retries regardless.
func (r *Relay) SetOnError(fn func(error)) {
	r.onError = fn
}

// ProcessBatch runs the three-phase A/B/C cycle once:
//
//	A) Short tx: fetch up to BatchSize unpublished rows (FOR UPDATE SKIP LOCKED)
//	   into memory and commit — locks released immediately.
//	B) No tx: publish all rows to the transport (batched if possible).
//	C) Short tx: mark all published rows in one batch UPDATE.
//
// Returns the number of rows successfully published. If publish (phase B)
// fails, phase C is skipped and all rows remain unpublished for retry.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	// Phase A: short claim transaction — fetch and immediately commit.
	// SKIP LOCKED prevents concurrent relays from fetching the same rows
	// within the same phase-A window; once this tx commits the locks are
	// released. A second relay may then re-claim the same rows before phase C
	// marks them → duplicate publish → inbox deduplicates by Message.ID.
	var msgs []Message
	if err := pg.RunInTx(ctx, r.pool, func(txCtx context.Context) error {
		q := gen.New(pg.FromContext(txCtx, r.pool))
		rows, err := q.FetchUnpublished(txCtx, r.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("outbox: fetch: %w", err)
		}
		msgs = make([]Message, len(rows))
		for i, row := range rows {
			msgs[i] = Message{
				ID:            row.ID,
				AggregateType: row.AggregateType,
				AggregateID:   row.AggregateID,
				EventType:     row.EventType,
				Payload:       row.Payload,
				Headers:       []byte(row.Headers),
				CreatedAt:     row.CreatedAt.Time,
			}
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("outbox: claim phase: %w", err)
	}

	if len(msgs) == 0 {
		return 0, nil
	}

	// Phase B: publish outside any transaction.
	// Use BatchPublisher if the injected Publisher also implements it —
	// one broker round-trip (Flush) for the whole batch instead of N×ProduceSync.
	// Fall back to per-record Publish for plain Publisher implementations.
	if bp, ok := r.pub.(BatchPublisher); ok {
		if err := bp.PublishBatch(ctx, msgs); err != nil {
			return 0, fmt.Errorf("outbox: batch publish: %w", err)
		}
	} else {
		for _, msg := range msgs {
			if err := r.pub.Publish(ctx, msg); err != nil {
				return 0, fmt.Errorf("outbox: publish %s: %w", msg.ID, err)
			}
		}
	}

	// Phase C: mark all successfully-published rows in one batch statement.
	ids := make([]uuid.UUID, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}
	q := gen.New(r.pool.Writer())
	if err := q.MarkPublishedBatch(ctx, ids); err != nil {
		return 0, fmt.Errorf("outbox: mark published batch: %w", err)
	}

	return len(msgs), nil
}

const (
	backoffBase = 100 * time.Millisecond
	backoffMax  = 30 * time.Second
)

// Run polls until ctx is canceled. On every tick it DRAINS the backlog:
// ProcessBatch is repeated as long as it returns a full batch, so a burst of
// N rows is published in ceil(N/BatchSize) consecutive batches within one
// tick instead of being capped at BatchSize rows per PollInterval. A partial
// batch ends the drain loop until the next tick (no busy loop when idle).
//
// On ProcessBatch errors it calls OnError (if set), ends the drain loop, and
// applies a simple capped exponential backoff before the next attempt
// (resets on success). Intended to be launched in a goroutine; returns
// ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	var consecutiveFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Drain: keep processing while full batches come back — there may
			// be more backlog waiting. Stop on a partial batch, error, or
			// context cancellation.
			var err error
			for {
				var n int
				n, err = r.ProcessBatch(ctx)
				if err != nil || n < int(r.cfg.BatchSize) || ctx.Err() != nil {
					break
				}
			}
			if err != nil {
				if r.onError != nil {
					r.onError(err)
				}
				consecutiveFailures++
				backoff := backoffBase
				for i := 1; i < consecutiveFailures; i++ {
					backoff *= 2
					if backoff > backoffMax {
						backoff = backoffMax
						break
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}
