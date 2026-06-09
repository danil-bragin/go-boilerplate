package service

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/outboxkafka"
)

// AddOutboxRelay wires an outbox relay + cleaner. Uses the passed publisher
// (typically outboxkafka.New(producer)). Must be called before Start.
func (s *Service) AddOutboxRelay(publisher outbox.Publisher, cfg outbox.RelayConfig) {
	relay := outbox.NewRelay(s.pool, publisher, cfg)
	relay.SetOnError(func(err error) {
		s.logger.Error("outbox relay error", "error", err)
	})

	cleaner := outbox.NewCleaner(s.pool)
	cleaner.SetOnError(func(err error) {
		s.logger.Error("outbox cleaner error", "error", err)
	})

	retention := cfg.RetentionAge
	if retention == 0 {
		retention = 24 * time.Hour
	}
	interval := cfg.CleanupInterval
	if interval == 0 {
		interval = time.Hour
	}

	s.goroutines = append(
		s.goroutines,
		func(ctx context.Context) {
			if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
				s.logger.Error("relay stopped unexpectedly", "error", err)
			}
		},
		func(ctx context.Context) {
			if err := cleaner.RunCleanup(ctx, interval, retention); err != nil && ctx.Err() == nil {
				s.logger.Error("cleaner stopped unexpectedly", "error", err)
			}
		},
	)
}

// DefaultOutboxPublisher builds the standard outboxkafka publisher backed by
// the service's producer. Convenience helper for services using the default
// topic-per-aggregate-type mapping.
func (s *Service) DefaultOutboxPublisher() outbox.Publisher {
	return outboxkafka.New(s.producer)
}
