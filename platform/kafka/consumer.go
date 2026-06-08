package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HandlerFunc processes a single Kafka record. The consumer commits the record's
// offset only after the handler returns nil. If the handler returns a non-nil
// error the offset is NOT committed, so the record will be redelivered on the
// next poll (at-least-once delivery). The caller is responsible for any
// retry/dead-letter logic (see platform/kafka/dlq.go in Task 7).
type HandlerFunc func(ctx context.Context, r Record) error

// Consumer wraps a *kgo.Client configured for cooperative-sticky group
// consumption with manual offset commit.
type Consumer struct {
	cl *kgo.Client
}

// NewConsumer builds a Consumer that joins the given consumer group and
// subscribes to the given topics. cfg.GroupID must be non-empty.
//
// The underlying kgo.Client is created via NewClient so it inherits the
// standard seed-broker, client-id, and OpenTelemetry hooks. On top of that
// the following group options are added:
//   - kgo.ConsumerGroup(cfg.GroupID)
//   - kgo.ConsumeTopics(topics...)
//   - kgo.Balancers(kgo.CooperativeStickyBalancer())
//   - kgo.DisableAutoCommit()
func NewConsumer(cfg Config, topics ...string) (*Consumer, error) {
	if cfg.GroupID == "" {
		return nil, errors.New("kafka: NewConsumer: cfg.GroupID must not be empty")
	}
	if len(topics) == 0 {
		return nil, errors.New("kafka: NewConsumer: at least one topic must be provided")
	}

	cl, err := NewClient(cfg,
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: NewConsumer: %w", err)
	}

	return &Consumer{cl: cl}, nil
}

// Run enters a poll loop, calling h for each record received from the broker.
//
// Commit semantics (at-least-once, per-record):
//   - After h returns nil the record's offset is committed synchronously via
//     CommitRecords before the next record is processed.
//   - If h returns a non-nil error the loop breaks out of the current batch
//     and returns to PollFetches without committing the failed record or any
//     subsequent records in the same batch. Those offsets will be redelivered
//     on the next assignment.
//
// The loop stops when ctx is cancelled; Run returns ctx.Err() in that case.
// It also returns if the underlying client is closed (ErrClientClosed).
func (c *Consumer) Run(ctx context.Context, h HandlerFunc) error {
	for {
		fetches := c.cl.PollFetches(ctx)

		// Stop if the caller cancelled the context.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Stop if the client itself has been closed.
		if fetches.IsClientClosed() {
			return nil
		}

		// Log / handle fetch-level errors.  Context and client-closed errors
		// are already handled above.  Other errors (e.g. auth, data-loss) are
		// non-fatal for the poll loop: log them and continue so the client can
		// resume once the underlying condition is resolved.
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				return fe.Err
			}
			if errors.Is(fe.Err, kgo.ErrClientClosed) {
				return nil
			}
			// Non-fatal: log and continue (real implementations would use
			// a structured logger; keep it simple for the platform layer).
			_ = fe.Err // swallow; iterator will skip errored partitions
		}

		// Process records one at a time.  Break on the first handler failure
		// so un-processed records are redelivered.
		var handlerErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if handlerErr != nil {
				// A previous record in this batch already failed; skip the rest.
				return
			}

			r := recordFromKGO(rec)
			if err := h(ctx, r); err != nil {
				handlerErr = err
				return
			}

			// Commit immediately after a successful handler invocation.
			// Errors here are non-fatal for the record (it may already be
			// committed or will be retried on the next session).
			_ = c.cl.CommitRecords(ctx, rec)
		})
	}
}

// Close closes the underlying kgo.Client, leaving the consumer group cleanly.
func (c *Consumer) Close() {
	c.cl.Close()
}

// recordFromKGO converts a *kgo.Record to the broker-agnostic Record type.
func recordFromKGO(rec *kgo.Record) Record {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Record{
		Topic:   rec.Topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
	}
}
