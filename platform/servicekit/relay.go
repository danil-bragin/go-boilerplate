package servicekit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/messaging/outboxkafka"
	"go-boilerplate/platform/messaging/serde"

	"google.golang.org/protobuf/proto"
)

// AddOutboxRelay wires an outbox relay + cleaner. Uses the passed publisher
// (typically outboxkafka.New(producer)). Must be called before Start.
//
// When cfg.SingleActive is true (the default, OUTBOX_SINGLE_ACTIVE env) the
// relay runs in advisory-lock leader mode: only one instance of the service
// publishes at a time, preserving per-aggregate event order across replicas.
// Set OUTBOX_SINGLE_ACTIVE=false only when consumers are reorder-safe.
//
// Returns an error when the service was built with WithoutPG — the outbox
// lives in Postgres.
func (s *Service) AddOutboxRelay(publisher outbox.Publisher, cfg outbox.RelayConfig) error {
	if s.pool == nil {
		return errors.New("servicekit: AddOutboxRelay requires postgres, but the service was built with WithoutPG()")
	}
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
	return nil
}

// DefaultOutboxPublisher builds the standard outboxkafka publisher backed by
// the service's producer. When SERDE_SR_URL is configured the publisher frames
// record values in the Confluent wire format via the service serde — register
// every produced event type with RegisterSchema BEFORE the relay starts.
func (s *Service) DefaultOutboxPublisher() outbox.Publisher {
	var opts []outboxkafka.Option
	if s.serde != nil {
		opts = append(opts, outboxkafka.WithEncoder(s.serde))
	}
	return outboxkafka.New(s.producer, opts...)
}

// Serde returns the Schema Registry serde, or nil when SERDE_SR_URL is unset.
func (s *Service) Serde() *serde.Serde { return s.serde }

// RegisterSchema registers msg's proto schema under "<topic>-value" and binds
// it to the versioned eventType (e.g. "orders.OrderCreated.v1") for both the
// outbox publisher (encode) and consumers (decode). No-op when SERDE_SR_URL
// is unset. Idempotent; fails fast on registry errors — call it during
// service wiring, before Start.
func (s *Service) RegisterSchema(ctx context.Context, topic, eventType string, msg proto.Message) error {
	if s.serde == nil {
		return nil
	}
	if err := s.serde.Register(ctx, topic+"-value", eventType, msg); err != nil {
		return fmt.Errorf("servicekit: register schema for %s on %s: %w", eventType, topic, err)
	}
	return nil
}
