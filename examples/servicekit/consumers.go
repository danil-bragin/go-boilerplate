package servicekit

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/kafka"
)

// EnsureTopics creates topics if they do not already exist (idempotent).
func (s *Service) EnsureTopics(ctx context.Context, partitions int32, rf int16, topics ...string) error {
	return kafka.EnsureTopics(ctx, s.kafkaClient, partitions, rf, topics...)
}

// AddConsumer wires a Kafka consumer: wraps handler with WithRetry (poison→DLT),
// creates a consumer for the group+topics, and registers its Run goroutine.
// DLT topics (topic+".DLT") are also created via EnsureTopics.
// Must be called before Start.
func (s *Service) AddConsumer(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc) error {
	// Ensure DLT topics exist alongside the source topics.
	allTopics := make([]string, 0, len(topics)*2)
	allTopics = append(allTopics, topics...)
	for _, t := range topics {
		allTopics = append(allTopics, t+".DLT")
	}
	if err := s.EnsureTopics(ctx, 1, 1, allTopics...); err != nil {
		return err
	}

	// Wrap with retry/DLT so poison messages never block the partition.
	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: 3,
		Producer:    s.producer,
		Backoff:     100 * time.Millisecond,
	})

	// Build consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics...)
	if err != nil {
		return err
	}
	// Register consumer Close in the closer (runs before consumers-cancel in LIFO,
	// but consumers-cancel is registered LAST so it runs FIRST — see ordering note at top).
	s.closer.Add("kafka-consumer-"+groupID, consumer.Close)

	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := consumer.Run(ctx, wrapped); err != nil && ctx.Err() == nil {
			s.logger.Error("consumer stopped unexpectedly", "group", groupID, "error", err)
		}
	})
	return nil
}
