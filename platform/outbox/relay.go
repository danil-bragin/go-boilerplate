package outbox

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/outbox/gen"
	"go-boilerplate/platform/pg"
)

// RelayConfig configures the polling relay.
type RelayConfig struct {
	BatchSize    int32         `env:"OUTBOX_BATCH_SIZE" envDefault:"100"`
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"1s"`
}

// Relay polls unpublished outbox rows and publishes them via a Publisher,
// marking each published only after a successful Publish. Each batch runs in a
// transaction using `for update skip locked`, so multiple relay instances can
// run concurrently without double-publishing.
//
// Delivery semantics: if Publish succeeds but the surrounding transaction's
// commit fails, the message is re-published on a later poll — delivery is
// AT-LEAST-ONCE. Consumers must be idempotent and deduplicate by Message.ID.
//
// ProcessBatch holds the row-lock transaction while publishing; with a slow
// broker this lengthens lock/connection hold time. SP3's Kafka publisher
// should bound publish latency and batch size accordingly.
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

// ProcessBatch fetches up to BatchSize unpublished messages (locking them),
// publishes each, and marks them published — all in one transaction. It
// returns the number of messages successfully published. A publish error
// aborts the batch (transaction rolls back), leaving rows unpublished for
// retry on the next poll — demonstrating at-least-once delivery semantics.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	var published int
	err := pg.RunInTx(ctx, r.pool, func(ctx context.Context) error {
		q := gen.New(pg.FromContext(ctx, r.pool))
		rows, err := q.FetchUnpublished(ctx, r.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("outbox: fetch: %w", err)
		}
		for _, row := range rows {
			msg := Message{
				ID:            row.ID,
				AggregateType: row.AggregateType,
				AggregateID:   row.AggregateID,
				EventType:     row.EventType,
				Payload:       row.Payload,
				Headers:       []byte(row.Headers),
				CreatedAt:     row.CreatedAt.Time,
			}
			if err := r.pub.Publish(ctx, msg); err != nil {
				return fmt.Errorf("outbox: publish %s: %w", msg.ID, err)
			}
			if err := q.MarkPublished(ctx, msg.ID); err != nil {
				return fmt.Errorf("outbox: mark published %s: %w", msg.ID, err)
			}
			published++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, nil
}

const (
	backoffBase = 100 * time.Millisecond
	backoffMax  = 30 * time.Second
)

// Run polls until ctx is canceled, processing a batch every PollInterval.
// On ProcessBatch errors it calls OnError (if set) and applies a simple
// capped exponential backoff before the next attempt (resets on success).
// Intended to be launched in a goroutine; returns ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	var consecutiveFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, err := r.ProcessBatch(ctx)
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
