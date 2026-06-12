package servicekit

import (
	"context"
	"fmt"
	"strconv"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"
)

// EnsureTopics creates topics if they do not already exist (idempotent).
// The topic spec (partitions, replication factor, retention.ms) comes from
// the service config (TOPIC_PARTITIONS / TOPIC_RF / TOPIC_RETENTION). When
// ENSURE_TOPICS=false this is a no-op — production topologies should manage
// topics as IaC instead of creating them from application startup.
func (s *Service) EnsureTopics(ctx context.Context, topics ...string) error {
	if !s.cfg.EnsureTopics {
		return nil
	}
	if s.kafkaClient == nil {
		return errNoKafka("EnsureTopics")
	}
	spec := kafka.TopicSpec{
		Partitions:        s.cfg.TopicPartitions,
		ReplicationFactor: s.cfg.TopicRF,
	}
	if s.cfg.TopicRetention > 0 {
		spec.Configs = map[string]string{
			"retention.ms": strconv.FormatInt(s.cfg.TopicRetention.Milliseconds(), 10),
		}
	}
	return kafka.EnsureTopics(ctx, s.kafkaClient, spec, topics...)
}

// AddConsumer wires a Kafka consumer: wraps handler with WithRetry (poison→DLT),
// creates a consumer for the group+topics, and registers its Run goroutine.
// DLT topics (topic+".DLT") are also created via EnsureTopics.
// Must be called before Start.
func (s *Service) AddConsumer(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc) error {
	if s.kafkaClient == nil {
		return errNoKafka("AddConsumer")
	}

	// Ensure DLT topics exist alongside the source topics.
	allTopics := make([]string, 0, len(topics)*2)
	allTopics = append(allTopics, topics...)
	for _, t := range topics {
		allTopics = append(allTopics, t+".DLT")
	}
	if err := s.EnsureTopics(ctx, allTopics...); err != nil {
		return err
	}

	// Wrap with retry/DLT so poison messages never block the partition.
	wrapped := kafka.WithRetry(handler, kafka.RetryOpts{
		MaxAttempts: s.cfg.ConsumerRetryMaxAttempts,
		Producer:    s.producer,
		Backoff:     s.cfg.ConsumerRetryBackoff,
	})

	// Build consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics, s.consumerOnError(groupID))
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

// AddBatchConsumer is AddConsumer for a batch handler: each partition's poll
// records are applied in one transaction (consume.Consumer.BatchHandler / the
// kafka RunBatch loop). Use it for high-volume idempotent projections where
// per-event commits are the bottleneck. Same lifecycle, group, topics, and
// error handling as AddConsumer.
func (s *Service) AddBatchConsumer(ctx context.Context, groupID string, topics []string, handler kafka.BatchHandlerFunc) error {
	if s.kafkaClient == nil {
		return errNoKafka("AddBatchConsumer")
	}

	// Ensure DLT topics exist alongside the source topics.
	allTopics := make([]string, 0, len(topics)*2)
	allTopics = append(allTopics, topics...)
	for _, t := range topics {
		allTopics = append(allTopics, t+".DLT")
	}
	if err := s.EnsureTopics(ctx, allTopics...); err != nil {
		return err
	}

	// Build consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics, s.consumerOnError(groupID))
	if err != nil {
		return err
	}
	// Register consumer Close in the closer (runs before consumers-cancel in LIFO,
	// but consumers-cancel is registered LAST so it runs FIRST — see ordering note at top).
	s.closer.Add("kafka-consumer-"+groupID, consumer.Close)

	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := consumer.RunBatch(ctx, handler); err != nil && ctx.Err() == nil {
			s.logger.Error("consumer stopped unexpectedly", "group", groupID, "error", err)
		}
	})
	return nil
}

// AddConsumerWithRetry wires a consumer whose failures escalate to tiered
// retry topics (non-blocking redrive) instead of blocking the partition
// with in-process backoff. The flow: policy.FastAttempts immediate
// in-process attempts → escalate to <topic>.retry.<idx> → retry consumer
// redelivers when due → after the last tier, <topic>.DLT.
// Services with strict latency/throughput needs should prefer this over
// AddConsumer; AddConsumer remains for simple consumers.
//
// ORDERING: tiered retry breaks per-key ordering unless the handler is
// reorder-safe or policy.KeyParkingWindow is set (best-effort key parking —
// see the retry package documentation for the full trade-off).
func (s *Service) AddConsumerWithRetry(ctx context.Context, groupID string, topics []string, handler kafka.HandlerFunc, policy retry.Policy) error {
	if s.kafkaClient == nil {
		return errNoKafka("AddConsumerWithRetry")
	}

	// 1. Provision all required topics: base + tier + DLT.
	allTopics := make([]string, 0, len(topics)*(2+len(policy.Tiers)))
	allTopics = append(allTopics, topics...)
	for _, base := range topics {
		for i := range policy.Tiers {
			allTopics = append(allTopics, retry.TierTopic(base, i))
		}
		allTopics = append(allTopics, retry.DLTTopic(base))
	}
	if err := s.EnsureTopics(ctx, allTopics...); err != nil {
		return err
	}

	// 2. Build the escalator backed by the service producer. NewEscalator
	// honors policy.KeyParkingWindow directly (key parking opt-in).
	esc := retry.NewEscalator(s.producer, policy)

	// 3. Wrap the handler: parked-key diversion + policy.FastAttempts
	// in-process attempts, then escalation to the next retry tier.
	wrapped := retry.WrapHandler(handler, esc, policy)

	// 4. Build and register the main consumer.
	consumerCfg := s.cfg.Kafka
	consumerCfg.GroupID = groupID
	consumer, err := kafka.NewConsumer(consumerCfg, topics, s.consumerOnError(groupID))
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

// consumerOnError returns the standard operational-error callback for kafka
// consumers: log with stage + group so persistent fetch/commit failures
// (which widen the duplicate-delivery window) are visible and alertable.
func (s *Service) consumerOnError(groupID string) kafka.ConsumerOption {
	return kafka.WithOnError(func(ctx context.Context, stage string, err error) {
		s.logger.ErrorContext(ctx, "kafka consumer error",
			"group", groupID, "stage", stage, "error", err)
	})
}

// errNoKafka is the uniform error for kafka-dependent methods on a Service
// built with WithoutKafka.
func errNoKafka(method string) error {
	return fmt.Errorf("servicekit: %s requires kafka, but the service was built with WithoutKafka()", method)
}
