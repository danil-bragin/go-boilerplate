// Package consume provides the standard typed Kafka consumer pipeline used by
// every service in this repository. It bundles the per-record concerns that
// were previously copy-pasted into each service transport:
//
//   - event-type header dispatch using versioned names ("orders.OrderCreated.v1");
//     unknown or missing event types are SKIPPED with a log line (forward
//     compatible — new producers must not break old consumers), never errored.
//   - payload decoding: Schema Registry wire format when a serde is injected
//     (WithSerde), raw proto.Unmarshal otherwise.
//   - uniform message-id policy: the "message-id" header (set by the outbox
//     publisher to the outbox row id) with a "topic:partition:offset" fallback
//     for records produced outside the outbox path.
//   - idempotency: the typed handler runs inside inbox.ProcessOnce, so the
//     side effect and the dedup marker commit atomically.
//   - principal propagation: principal-sub / principal-roles headers are
//     installed into ctx before the handler runs (see auth.ExtractToContext) —
//     audit trails record the real actor, not "anonymous".
//
// Usage:
//
//	handler := consume.New(pool, "gateway-projection", consume.WithLogger(l)).Handler(
//	    consume.TypedFor(1, onOrderCreated),     // event type derived from the proto message
//	    consume.TypedFor(1, onPaymentProcessed), // (see EventTypeFor)
//	)
//	svc.AddConsumer(ctx, "gateway-projection", topics, handler)
//
// Prefer TypedFor over Typed with a hand-written event-type string: the name
// is derived from the proto message via EventTypeFor, so producers and
// consumers can never drift.
package consume

import (
	"context"
	"fmt"
	"log/slog"

	"go-boilerplate/platform/messaging/inbox"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/serde"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/pg"

	"google.golang.org/protobuf/proto"
)

// srMagicByte is the leading byte of the Confluent Schema Registry wire
// format ([0x00][4-byte schema id][payload]). Used to detect producer/
// consumer serde mismatches with a pointed error instead of a generic
// decode failure.
const srMagicByte = 0x00

// Consumer builds kafka.HandlerFuncs that share a pool, inbox consumer-group
// name, decoder, and logger.
type Consumer struct {
	pool    *pg.Pool
	group   string
	dec     serde.Deserializer // nil → raw proto.Unmarshal
	logger  *slog.Logger
	noInbox bool // test-only: skip inbox.ProcessOnce (see WithoutInbox)
}

// Option configures a Consumer.
type Option func(*Consumer)

// WithSerde injects a Schema Registry deserializer; record values are then
// expected in the Confluent wire format. Without it, values are decoded with
// plain proto.Unmarshal. Must match the producer side (SERDE_SR_URL).
func WithSerde(d serde.Deserializer) Option {
	return func(c *Consumer) { c.dec = d }
}

// WithLogger sets the logger used for skip lines (default slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(c *Consumer) { c.logger = l }
}

// WithoutInbox disables the inbox.ProcessOnce wrapper: the typed handler runs
// directly, with NO database and NO dedup (pool may be nil).
//
// TEST-ONLY escape hatch. It exists so fast-lane unit tests can exercise the
// real decode → dispatch → handler pipeline through fakes.Broker without
// Docker. Production consumers MUST keep the inbox — without it, at-least-
// once delivery re-runs side effects on every redelivery.
func WithoutInbox() Option {
	return func(c *Consumer) { c.noInbox = true }
}

// New creates a Consumer. group is the inbox consumer name — messages are
// processed at most once per (group, message-id).
func New(pool *pg.Pool, group string, opts ...Option) *Consumer {
	c := &Consumer{pool: pool, group: group, logger: slog.Default()}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Handler is one typed event handler bound to a versioned event type.
// Construct with Typed.
type Handler interface {
	eventType() string
	handle(ctx context.Context, dec serde.Deserializer, value []byte) (onCommitted func(context.Context), err error)
}

type typedHandler[T proto.Message] struct {
	event       string
	fn          func(context.Context, T) error
	onCommitted func(context.Context, T)
}

// TypedOption configures a typed handler built by Typed or TypedFor.
type TypedOption[T proto.Message] func(*typedHandler[T])

// OnCommitted sets a callback that runs AFTER the inbox transaction commits
// successfully — use it for post-commit side effects such as cache
// invalidation that must not run before the write is visible. It does not
// run for duplicate messages (the original delivery already ran it) nor when
// the transaction rolled back. When given multiple times, the last call wins.
func OnCommitted[T proto.Message](fn func(context.Context, T)) TypedOption[T] {
	return func(h *typedHandler[T]) { h.onCommitted = fn }
}

// Typed builds a Handler that decodes the record value into T and invokes fn
// inside the inbox transaction. Options (e.g. OnCommitted) configure
// post-commit behavior.
func Typed[T proto.Message](
	eventType string,
	fn func(context.Context, T) error,
	opts ...TypedOption[T],
) Handler {
	h := typedHandler[T]{event: eventType, fn: fn}
	for _, o := range opts {
		o(&h)
	}
	return h
}

func (h typedHandler[T]) eventType() string { return h.event }

func (h typedHandler[T]) handle(ctx context.Context, dec serde.Deserializer, value []byte) (func(context.Context), error) {
	// Instantiate a fresh T (T is a pointer type; ProtoReflect works on the
	// zero value and gives us the message type to allocate).
	var zero T
	msg, ok := zero.ProtoReflect().Type().New().Interface().(T)
	if !ok {
		return nil, fmt.Errorf("consume: cannot instantiate message for %s", h.event)
	}

	if dec != nil {
		if err := dec.Decode(value, msg); err != nil {
			// Mismatch diagnostic: this consumer expects Confluent-SR framed
			// values, but the record has no 0x00 magic byte — almost
			// certainly a producer publishing raw protobuf. Without the hint
			// every record decode-errors into the DLT until an operator
			// correlates the two configs.
			if len(value) == 0 || value[0] != srMagicByte {
				return nil, fmt.Errorf("consume: decode %s: value is not Confluent-SR framed (no 0x00 magic byte) — producer publishing raw protobuf while this consumer has SERDE_SR_URL set? %w", h.event, err)
			}
			return nil, fmt.Errorf("consume: decode %s: %w", h.event, err)
		}
	} else {
		// Inverse mismatch: a valid protobuf message never starts with 0x00
		// (field number 0 is illegal), so a leading magic byte on a value
		// long enough to hold the 5-byte SR header + payload means the
		// producer frames with Schema Registry while this consumer decodes
		// raw protobuf.
		if len(value) >= 6 && value[0] == srMagicByte {
			return nil, fmt.Errorf("consume: unmarshal %s: value appears Confluent-SR framed (0x00 magic byte) but this consumer decodes raw protobuf — SERDE_SR_URL mismatch between producer and consumer?", h.event)
		}
		if err := proto.Unmarshal(value, msg); err != nil {
			return nil, fmt.Errorf("consume: unmarshal %s: %w", h.event, err)
		}
	}

	if err := h.fn(ctx, msg); err != nil {
		return nil, err
	}
	if h.onCommitted == nil {
		return noopCommitted, nil
	}
	return func(ctx context.Context) { h.onCommitted(ctx, msg) }, nil
}

// noopCommitted is the post-commit callback for handlers without one; a
// non-nil sentinel keeps the (callback, error) return unambiguous.
func noopCommitted(context.Context) {}

// Handler combines the given typed handlers into a single kafka.HandlerFunc
// that dispatches on the "event-type" record header. Records whose event type
// matches none of the handlers are skipped with a debug log line and nil
// error (committed, never redelivered): unknown events are a forward-
// compatibility situation, not a failure.
func (c *Consumer) Handler(handlers ...Handler) kafka.HandlerFunc {
	byType := make(map[string]Handler, len(handlers))
	for _, h := range handlers {
		if _, dup := byType[h.eventType()]; dup {
			panic(fmt.Sprintf("consume: duplicate handler for event type %q", h.eventType()))
		}
		byType[h.eventType()] = h
	}

	return func(ctx context.Context, r kafka.Record) error {
		// Install the propagated principal (if any) so audit behaviors see
		// the real actor. Transport metadata, not authentication — the trust
		// boundary is the broker ACL/mTLS perimeter (see auth.ExtractToContext).
		ctx = auth.ExtractToContext(ctx, r.Headers)

		eventType := r.Headers[kafka.HeaderEventType]
		h, ok := byType[eventType]
		if !ok {
			c.logger.DebugContext(ctx, "consume: skipping unknown event type",
				"event_type", eventType, "topic", r.Topic, "group", c.group)
			return nil
		}

		// Uniform message-id policy: outbox-stamped header first, stable
		// topic:partition:offset position as the fallback.
		msgID := r.Headers[kafka.HeaderMessageID]
		if msgID == "" {
			msgID = fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset)
		}

		// Chain lineage: propagate the correlation id (falling back to this
		// record's message id when it starts a chain) and make this message
		// the causation parent of everything the handler emits. The outbox
		// repository stamps both onto enqueued messages automatically.
		corrID := r.Headers[msgctx.HeaderCorrelationID]
		if corrID == "" {
			corrID = msgID
		}
		ctx = msgctx.WithCorrelationID(ctx, corrID)
		ctx = msgctx.WithParentMessageID(ctx, msgID)

		if c.noInbox {
			// Test-only path (WithoutInbox): no transaction, no dedup.
			onCommitted, err := h.handle(ctx, c.dec, r.Value)
			if err != nil {
				return fmt.Errorf("consume: process %s (id=%s): %w", eventType, msgID, err)
			}
			if onCommitted != nil {
				onCommitted(ctx)
			}
			return nil
		}

		var onCommitted func(context.Context)
		_, err := inbox.ProcessOnce(ctx, c.pool, c.group, msgID, func(ctx context.Context) error {
			var err error
			onCommitted, err = h.handle(ctx, c.dec, r.Value)
			return err
		})
		if err != nil {
			return fmt.Errorf("consume: process %s (id=%s): %w", eventType, msgID, err)
		}
		if onCommitted != nil {
			onCommitted(ctx)
		}
		return nil
	}
}
