package servicekit

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"
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

// AddConsumerWithRetry wires a consumer whose failures escalate to tiered
// retry topics (non-blocking redrive) instead of blocking the partition
// with in-process backoff. The flow: policy.FastAttempts immediate
// in-process attempts → escalate to <topic>.retry.<tier> → retry consumer
// redelivers when due → after the last tier, <topic>.DLT.
// Services with strict latency/throughput needs should prefer this over
// AddConsumer; AddConsumer remains for simple consumers.
func (s *Service) AddConsumerWithRetry(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc, policy retry.Policy) error {
	// 1. Provision all required topics: base + tier + DLT.
	allTopics := make([]string, 0, len(topics)*(2+len(policy.Tiers)))
	allTopics = append(allTopics, topics...)
	for _, base := range topics {
		for _, d := range policy.Tiers {
			allTopics = append(allTopics, retry.TierTopic(base, d))
		}
		allTopics = append(allTopics, retry.DLTTopic(base))
	}
	if err := s.EnsureTopics(ctx, 1, 1, allTopics...); err != nil {
		return err
	}

	// 2. Build the escalator backed by the service producer.
	esc := retry.NewEscalator(s.producer, policy)

	// 3. Wrap the handler: policy.FastAttempts in-process attempts, then escalate.
	fastAttempts := policy.FastAttempts
	if fastAttempts <= 0 {
		fastAttempts = 1
	}
	wrapped := func(ctx context.Context, rec kafka.Record) error {
		var lastErr error
		for attempt := 1; attempt <= fastAttempts; attempt++ {
			lastErr = handler(ctx, rec)
			if lastErr == nil {
				return nil
			}
			// Sleep between attempts (consistent with AddConsumer's WithRetry backoff),
			// but only when there are more attempts remaining.
			if attempt < fastAttempts {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		// All fast attempts exhausted — escalate to the next retry tier.
		// Escalation failure: return the error so the record is NOT committed
		// and will redeliver. Never drop a record.
		if _, err := esc.Escalate(ctx, rec.Topic, rec, lastErr); err != nil {
			return err
		}
		// Successful escalation: return nil so the consumer commits the offset.
		// The record will be redelivered by the retry consumer when its due time arrives.
		return nil
	}

	// 4. Build and register the main consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics...)
	if err != nil {
		return err
	}
	s.closer.Add("kafka-consumer-"+groupID, consumer.Close)
	s.goroutines = append(s.goroutines, func(runCtx context.Context) {
		if err := consumer.Run(runCtx, wrapped); err != nil && runCtx.Err() == nil {
			s.logger.Error("consumer stopped unexpectedly", "group", groupID, "error", err)
		}
	})

	// 5. Build and register the retry consumer (redelivers tier-topic records to the raw handler).
	retryGroupID := groupID + ".retry"
	rc, err := retry.NewConsumer(
		s.cfg.Kafka,
		retryGroupID,
		topics,
		handler,
		esc,
		policy,
		retry.WithLogger(s.logger),
	)
	if err != nil {
		return err
	}
	s.closer.Add("retry-consumer-"+groupID, rc.Close)
	s.goroutines = append(s.goroutines, func(runCtx context.Context) {
		if err := rc.Run(runCtx); err != nil && runCtx.Err() == nil {
			s.logger.Error("retry consumer stopped unexpectedly", "group", retryGroupID, "error", err)
		}
	})

	return nil
}
