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
// When cfg.Shards > 1 (OUTBOX_RELAY_SHARDS) it instead wires a FLEET of N
// leader-elected shard relays (ADR-0017): each owns the aggregate_ids that hash
// into its shard and holds its own advisory lock, so up to N instances publish
// concurrently while per-aggregate order is preserved (a key maps to one
// shard, whose single leader publishes it in order). Sharding implies
// per-shard leader election regardless of SingleActive. The publisher is shared
// across shards (the franz-go producer is goroutine-safe). One cleaner runs per
// instance regardless of shard count (age-based DELETE is shard-agnostic).
//
// Returns an error when the service was built with WithoutPG — the outbox
// lives in Postgres.
//
// PHYSICAL SHARDING (Tier-3, ADR-0019): the outbox lives in EACH physical
// database, so a relay fleet is wired PER physical shard. An order whose
// order_id hashes to physical shard 1 writes its OrderCreated to shard 1's
// outbox; without a per-shard relay that row would never be relayed and the
// choreography would stall. Each physical shard gets its own leader lock
// (":pshard:<i>" suffix) so shard 0's leader and shard 1's leader do not
// collide. At M=1 (PG_SHARDS unset) there is one physical shard, no suffix is
// appended, and the wiring — including the leader-lock key — is byte-identical
// to the historical single-pool behaviour.
func (s *Service) AddOutboxRelay(publisher outbox.Publisher, cfg outbox.RelayConfig) error {
	if s.pool == nil {
		return errors.New("servicekit: AddOutboxRelay requires postgres, but the service was built with WithoutPG()")
	}

	shards := cfg.Shards
	if shards < 1 {
		shards = 1
	}

	physical := s.shards.Shards()
	multiPhysical := len(physical) > 1

	for pIdx, pool := range physical {
		// Per-physical-shard leader-key suffix: only set when M>1 so the M=1
		// lock key stays byte-identical to the historical bare key.
		var keySuffix string
		if multiPhysical {
			keySuffix = fmt.Sprintf(":pshard:%d", pIdx)
		}
		for i := range shards {
			var opts []outbox.RelayOption
			if shards > 1 {
				// Aggregate-sharded: each shard is its own leader (ordered parallel publish).
				opts = append(opts, outbox.WithSingleActive(pool.Writer()), outbox.WithShard(i, shards))
			} else if cfg.SingleActive {
				opts = append(opts, outbox.WithSingleActive(pool.Writer()))
			}
			if keySuffix != "" {
				opts = append(opts, outbox.WithLeaderKeySuffix(keySuffix))
			}
			physIdx, shardIdx := pIdx, i
			relay := outbox.NewRelay(pool, publisher, cfg, opts...)
			relay.SetOnError(func(err error) {
				s.logger.Error("outbox relay error", "pshard", physIdx, "shard", shardIdx, "shards", shards, "error", err)
			})
			s.goroutines = append(s.goroutines, func(ctx context.Context) {
				if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
					s.logger.Error("relay stopped unexpectedly", "pshard", physIdx, "shard", shardIdx, "error", err)
				}
			})
		}

		// One cleaner per physical shard: the age-based DELETE is shard-agnostic
		// within a database but must run on EVERY database, else only shard 0's
		// published rows are ever pruned.
		cleaner := outbox.NewCleaner(pool)
		physIdx := pIdx
		cleaner.SetOnError(func(err error) {
			s.logger.Error("outbox cleaner error", "pshard", physIdx, "error", err)
		})

		retention := cfg.RetentionAge
		if retention == 0 {
			retention = 24 * time.Hour
		}
		interval := cfg.CleanupInterval
		if interval == 0 {
			interval = time.Hour
		}

		s.goroutines = append(s.goroutines, func(ctx context.Context) {
			if err := cleaner.RunCleanup(ctx, interval, retention); err != nil && ctx.Err() == nil {
				s.logger.Error("cleaner stopped unexpectedly", "pshard", physIdx, "error", err)
			}
		})
	}
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
