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
	BatchSize    int32         `env:"OUTBOX_BATCH_SIZE" env-default:"100"`
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" env-default:"1s"`
}

// Relay polls unpublished outbox rows and publishes them via a Publisher,
// marking each published only after a successful Publish. Each batch runs in a
// transaction using `for update skip locked`, so multiple relay instances can
// run concurrently without double-publishing.
type Relay struct {
	pool *pg.Pool
	pub  Publisher
	cfg  RelayConfig
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

// ProcessBatch fetches up to BatchSize unpublished messages (locking them),
// publishes each, and marks them published — all in one transaction. It
// returns the number of messages successfully published. A publish error
// aborts the batch (transaction rolls back), leaving rows unpublished for
// retry on the next poll.
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

// Run polls until ctx is canceled, processing a batch every PollInterval.
// Intended to be launched in a goroutine; returns ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.ProcessBatch(ctx); err != nil {
				// Transient: swallow to keep the loop alive.
				// SP5 wires a real logger + metrics here.
				continue
			}
		}
	}
}
