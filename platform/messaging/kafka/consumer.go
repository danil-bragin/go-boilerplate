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

	cl, err := NewClient(
		cfg,
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

// Close closes the underlying kgo.Client, leaving the consumer group cleanly.
// The ctx parameter is accepted for interface consistency with run.TeardownFunc
// ("resources expose Close(ctx context.Context) error to register directly as
// a run.TeardownFunc") but is not used because kgo.Client.Close is synchronous.
func (c *Consumer) Close(_ context.Context) error {
	c.cl.Close()
	return nil
}
