package serde

// Serde implements Serializer/Deserializer using the Confluent Schema
// Registry wire format via franz-go's pkg/sr:
//
//	[0x00][4-byte big-endian schema id][protobuf message-index varint(s)][proto bytes]
//
// Schema source: the EXACT .proto source text compiled by buf, read from the
// embedded FS in the repository's proto package. A message's FileDescriptor
// Path() (e.g. "orders/v1/orders.proto") doubles as the embed path, so no
// schema printing/reconstruction is involved and the registered schema can
// never drift from the generated Go types. Well-known-type imports
// (google/protobuf/timestamp.proto etc.) need no explicit schema references:
// Redpanda SR ships the WKTs built in for the protobuf schema type.
//
// Registration (Register) is idempotent: the schema is first looked up by
// content (LookupSchema); only when the subject does not yet contain this
// exact schema is CreateSchema called. The caller's context is honoured
// throughout and any registry error is returned as-is — callers are expected
// to fail fast at startup when SR is enabled but unreachable.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	protofs "go-boilerplate/proto"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ErrUnknownSchema is returned by Decode when the wire header references a
// schema id (or message index) that has not been registered with this Serde
// instance. Typical causes: a producer registered a newer schema version, or
// the consumer forgot to Register the event type at startup.
var ErrUnknownSchema = errors.New("serde: unknown schema id")

// wireInfo is the cached wire header data for one registered event type.
type wireInfo struct {
	id    int
	index []int
}

// Serde encodes/decodes proto messages with a Confluent SR wire header.
// Construct with New, then Register every event type the service produces or
// consumes (idempotent). Safe for concurrent use after registration.
type Serde struct {
	client *sr.Client
	serde  *sr.Serde
	header sr.ConfluentHeader

	mu          sync.RWMutex
	byEventType map[string]wireInfo
}

// New creates a Serde backed by the Schema Registry at srURL. No network
// calls happen here; schemas are registered (or looked up) by Register.
func New(srURL string) (*Serde, error) {
	client, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		return nil, fmt.Errorf("serde: sr client: %w", err)
	}
	return &Serde{
		client:      client,
		serde:       sr.NewSerde(sr.Header(&sr.ConfluentHeader{})),
		byEventType: map[string]wireInfo{},
	}, nil
}

// Register makes msg's schema known to the registry under subject and wires
// encode/decode for it, recording the wire header under eventType (the
// versioned event-type name carried in Kafka headers, e.g.
// "orders.OrderCreated.v1") for EncodeValue.
//
// Idempotent: an exact-content match already present under the subject is
// reused (no new version is created). ctx is honoured for all registry calls;
// any error is returned so callers can fail fast at startup.
func (s *Serde) Register(ctx context.Context, subject, eventType string, msg proto.Message) error {
	desc := msg.ProtoReflect().Descriptor()
	path := desc.ParentFile().Path()

	src, err := protofs.FS.ReadFile(path)
	if err != nil {
		return fmt.Errorf("serde: embedded proto source %q: %w", path, err)
	}
	schema := sr.Schema{Schema: string(src), Type: sr.TypeProtobuf}

	// Look up by content first; only create when this exact schema is not yet
	// registered under the subject (idempotent re-registration).
	ss, err := s.client.LookupSchema(ctx, subject, schema)
	if err != nil {
		ss, err = s.client.CreateSchema(ctx, subject, schema)
		if err != nil {
			return fmt.Errorf("serde: register schema %q: %w", subject, err)
		}
	}

	index := messageIndex(desc)
	s.serde.Register(
		ss.ID,
		msg,
		sr.EncodeFn(func(v any) ([]byte, error) {
			m, ok := v.(proto.Message)
			if !ok {
				return nil, fmt.Errorf("serde: EncodeFn: expected proto.Message, got %T", v)
			}
			return proto.Marshal(m)
		}),
		sr.DecodeFn(func(b []byte, v any) error {
			m, ok := v.(proto.Message)
			if !ok {
				return fmt.Errorf("serde: DecodeFn: expected proto.Message, got %T", v)
			}
			return proto.Unmarshal(b, m)
		}),
		sr.Index(index...),
	)

	s.mu.Lock()
	s.byEventType[eventType] = wireInfo{id: ss.ID, index: index}
	s.mu.Unlock()
	return nil
}

// Encode marshals msg and prepends the Confluent SR wire header. The message
// type must have been registered via Register.
func (s *Serde) Encode(msg proto.Message) ([]byte, error) {
	b, err := s.serde.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("serde: encode: %w", err)
	}
	return b, nil
}

// EncodeValue prepends the Confluent SR wire header for the given registered
// event type to an already proto-marshalled payload. This is the publish-side
// fast path for the outbox relay, whose payloads are stored pre-marshalled:
// no unmarshal/re-marshal round trip is needed to frame them.
func (s *Serde) EncodeValue(eventType string, payload []byte) ([]byte, error) {
	s.mu.RLock()
	info, ok := s.byEventType[eventType]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("serde: event type %q not registered", eventType)
	}
	b, err := s.header.AppendEncode(make([]byte, 0, 8+len(payload)), info.id, info.index)
	if err != nil {
		return nil, fmt.Errorf("serde: encode header: %w", err)
	}
	return append(b, payload...), nil
}

// Decode strips the Confluent SR wire header and unmarshals the payload into
// into. A wire header referencing an unregistered schema id yields
// ErrUnknownSchema.
func (s *Serde) Decode(data []byte, into proto.Message) error {
	if err := s.serde.Decode(data, into); err != nil {
		if errors.Is(err, sr.ErrNotRegistered) {
			return fmt.Errorf("%w: %w", ErrUnknownSchema, err)
		}
		return fmt.Errorf("serde: decode: %w", err)
	}
	return nil
}

// messageIndex computes the Confluent protobuf message index for desc: the
// path of declaration indexes from the file's top-level messages down to the
// (possibly nested) message.
func messageIndex(desc protoreflect.MessageDescriptor) []int {
	var rev []int
	for d := protoreflect.Descriptor(desc); ; {
		parent := d.Parent()
		if parent == nil {
			break
		}
		rev = append(rev, d.Index())
		if _, isFile := parent.(protoreflect.FileDescriptor); isFile {
			break
		}
		d = parent
	}
	// Reverse: outermost first.
	idx := make([]int, len(rev))
	for i, v := range rev {
		idx[len(rev)-1-i] = v
	}
	return idx
}
