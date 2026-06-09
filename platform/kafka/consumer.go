package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HandlerFunc processes a single Kafka record. The consumer commits the record's
// offset only after the handler returns nil. If the handler returns a non-nil
// error the offset is NOT committed, so the record will be redelivered on the
// next poll (at-least-once delivery). The caller is responsible for any
// retry/dead-letter logic (see platform/kafka/dlq.go in Task 7).
type HandlerFunc func(ctx context.Context, r Record) error

// Consumer wraps a *kgo.Client configured for cooperative-sticky group
// consumption with manual offset commit and per-partition parallel processing.
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
//   - kgo.BlockRebalanceOnPoll() — prevents a rebalance from occurring between
//     PollFetches and AllowRebalance, so concurrent partition processing cannot
//     accidentally commit offsets for partitions that have been revoked.
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
		kgo.BlockRebalanceOnPoll(),
		// A brand-new consumer group (no committed offset yet) reads from the
		// START of its topics, so it never misses events produced during its
		// own startup window (e.g. a command published just before the group
		// finishes joining). Once the group has committed offsets, those take
		// precedence and this reset no longer applies — so it does NOT cause
		// reprocessing on restart.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: NewConsumer: %w", err)
	}

	return &Consumer{cl: cl}, nil
}

// Run enters a poll loop, processing partitions concurrently within each poll
// and committing all successful offsets in a single batch RPC per poll.
//
// Concurrency model:
//   - Each call to PollFetches may return records from multiple partitions.
//   - One goroutine is launched per partition (via sync.WaitGroup). Within a
//     partition records are processed SEQUENTIALLY, preserving per-partition
//     ordering.
//   - After all partition goroutines finish, the last successfully-handled
//     record from EACH partition is committed in a single CommitRecords call
//     (one OffsetCommit RPC for the whole poll instead of one per record).
//   - kgo.BlockRebalanceOnPoll() ensures no rebalance can occur between
//     PollFetches and AllowRebalance(), so concurrent processing is safe.
//     AllowRebalance() is called after every commit, even on error.
//
// Failure semantics (at-least-once, per-partition):
//   - Within a partition, on the first handler error processing stops for that
//     partition. The failing record and all subsequent records in that partition
//     are NOT committed and will be redelivered. Other partitions are
//     unaffected and commit their own progress independently.
//
// Duplicate-delivery window:
//   - There is a small window between a successful handler return and the
//     CommitRecords RPC completing. If the process crashes or a rebalance
//     occurs in that window the record will be redelivered after the partition
//     is reassigned. Consumers MUST be idempotent and deduplicate by a stable
//     idempotency key (e.g. an inbox table keyed on message-id).
//
// The loop stops when ctx is cancelled; Run returns ctx.Err() in that case.
// It also returns if the underlying client is closed (ErrClientClosed).
func (c *Consumer) Run(ctx context.Context, h HandlerFunc) error {
	for {
		fetches := c.cl.PollFetches(ctx)

		// Stop if the caller cancelled the context.
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			return ctx.Err()
		}

		// Stop if the client itself has been closed.
		if fetches.IsClientClosed() {
			c.cl.AllowRebalance()
			return nil
		}

		// Handle fetch-level errors. Context and client-closed errors are
		// already handled above. Other errors are non-fatal for the poll loop.
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				c.cl.AllowRebalance()
				return fe.Err
			}
			if errors.Is(fe.Err, kgo.ErrClientClosed) {
				c.cl.AllowRebalance()
				return nil
			}
			// Non-fatal: swallow; errored partitions are skipped by the iterator.
			_ = fe.Err
		}

		// Collect the last successfully-handled record per partition.
		// mu guards lastGood; each goroutine writes to its own slot after
		// finishing, so contention is minimal.
		var (
			mu       sync.Mutex
			lastGood []*kgo.Record
		)

		// Launch one goroutine per partition; preserve per-partition ordering.
		var wg sync.WaitGroup

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// Capture by value so the closure is safe to run concurrently.
			part := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				var (
					last   *kgo.Record
					failed bool
				)
				part.EachRecord(func(rec *kgo.Record) {
					if failed {
						// A previous record in this partition failed; skip the rest
						// so the offset does not advance past the failure point.
						return
					}
					r := recordFromKGO(rec)
					if err := h(ctx, r); err != nil {
						// Stop processing this partition; the failing record and
						// everything after it will be redelivered.
						failed = true
						return
					}
					last = rec
				})
				if last != nil {
					mu.Lock()
					lastGood = append(lastGood, last)
					mu.Unlock()
				}
			}()
		})

		// Wait for all partition goroutines to finish.
		wg.Wait()

		// Commit all successfully-processed partitions in one RPC.
		if len(lastGood) > 0 {
			_ = c.cl.CommitRecords(ctx, lastGood...)
		}

		// Allow the next rebalance now that we have committed.
		c.cl.AllowRebalance()
	}
}

// Close closes the underlying kgo.Client, leaving the consumer group cleanly.
// The ctx parameter is accepted for interface consistency with run.TeardownFunc
// ("resources expose Close(ctx context.Context) error to register directly as
// a run.TeardownFunc") but is not used because kgo.Client.Close is synchronous.
func (c *Consumer) Close(_ context.Context) error {
	c.cl.Close()
	return nil
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
