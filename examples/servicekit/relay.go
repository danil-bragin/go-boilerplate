package servicekit

import (
	"context"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/outboxkafka"
)

// AddOutboxRelay wires an outbox relay + cleaner. Uses the passed publisher
// (typically outboxkafka.New(producer)). Must be called before Start.
//
// When cfg.SingleActive is true (the default, OUTBOX_SINGLE_ACTIVE env) the
// relay runs in advisory-lock leader mode: only one instance of the service
// publishes at a time, preserving per-aggregate event order across replicas.
// Set OUTBOX_SINGLE_ACTIVE=false only when consumers are reorder-safe.
func (s *Service) AddOutboxRelay(publisher outbox.Publisher, cfg outbox.RelayConfig) {
	var opts []outbox.RelayOption
	if cfg.SingleActive {
		opts = append(opts, outbox.WithSingleActive(s.pool.Writer()))
	}
	relay := outbox.NewRelay(s.pool, publisher, cfg, opts...)
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
