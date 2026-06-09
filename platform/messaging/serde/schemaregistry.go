package serde

// SchemaRegistry implements Serializer and Deserializer using the Confluent
// Schema Registry wire format:
//
//	[0x00][4-byte big-endian schema id][protobuf message-index varint(s)][proto bytes]
//
// Implementation approach:
//
//	We use github.com/twmb/franz-go/pkg/sr (v1.7.0):
//	  - sr.NewClient(sr.URLs(srURL)) to talk to the registry.
//	  - sr.NewSerde(sr.Header(&sr.ConfluentHeader{})) as the wire-format helper.
//	  - Client.CreateSchema to register the schema and obtain its global ID.
//	  - Serde.Register(id, msg, sr.EncodeFn/DecodeFn/Index(0)) to wire proto <-> wire bytes.
//	  - Serde.Encode / Serde.Decode satisfy our Serializer / Deserializer interfaces.
//
// Schema text approach:
//
//	We derive the full, canonical proto3 source text for the message's .proto
//	file using github.com/jhump/protoreflect/desc/protoprint.Printer. This
//	produces a correct source including imports, enums, nested messages and
//	well-known types (WKT). Redpanda's Schema Registry has the WKT
//	(google/protobuf/timestamp.proto etc.) built in for the Protobuf schema
//	type, so no explicit schema references are needed — registering the
//	canonical source that contains `import "google/protobuf/timestamp.proto";`
//	is sufficient and the registry resolves that reference itself.
//
//	Protobuf message-index: for a top-level message that is the first (or only)
//	message in its .proto file, the Confluent index is [0], encoded as a single
//	zero-byte varint pair per the wire-format spec.

import (
	"context"
	"fmt"

	// desc is the legacy jhump API required by protoprint.Printer; its
	// deprecation note refers to using it in place of protoreflect, not to
	// using it as a bridge for the printer. There is no way to use
	// protoprint.Printer without desc.FileDescriptor, so the import is
	// intentional and the staticcheck warning is suppressed.
	"github.com/jhump/protoreflect/desc"            //nolint:staticcheck
	"github.com/jhump/protoreflect/desc/protoprint" //nolint:staticcheck
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SchemaRegistry encodes/decodes proto messages with a Confluent SR wire header.
type SchemaRegistry struct {
	serde    *sr.Serde
	msgType  protoreflect.MessageType
	schemaID int
}

// NewSchemaRegistry creates a SchemaRegistry serde for the given subject and
// proto message type. It registers the schema with the registry on construction
// and wires the sr.Serde for subsequent encode/decode calls.
//
// The full canonical .proto source for the message's file is printed using
// jhump/protoreflect protoprint.Printer, which correctly renders imports,
// enums, nested messages, and well-known types. Redpanda SR has WKTs built
// in for the Protobuf schema type, so no explicit schema references need to
// be registered for google/protobuf/*.proto.
func NewSchemaRegistry(srURL string, subject string, msg proto.Message) (*SchemaRegistry, error) {
	// Build SR client.
	client, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		return nil, fmt.Errorf("serde: sr client: %w", err)
	}

	// Derive the full, canonical proto source from the message's FileDescriptor.
	// WrapFile converts the modern protoreflect.FileDescriptor to the legacy
	// *desc.FileDescriptor that protoprint.Printer requires.
	fileDesc, err := desc.WrapFile(msg.ProtoReflect().Descriptor().ParentFile())
	if err != nil {
		return nil, fmt.Errorf("serde: wrap file descriptor: %w", err)
	}

	schemaText, err := (&protoprint.Printer{}).PrintProtoToString(fileDesc)
	if err != nil {
		return nil, fmt.Errorf("serde: print proto schema: %w", err)
	}

	// Register (or look up) the schema, obtaining the globally-unique schema ID.
	// Redpanda SR resolves WKT imports (google/protobuf/timestamp.proto etc.)
	// internally — no explicit schema references are required.
	ss, err := client.CreateSchema(context.Background(), subject, sr.Schema{
		Schema: schemaText,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return nil, fmt.Errorf("serde: register schema %q: %w", subject, err)
	}

	msgType := msg.ProtoReflect().Type()

	// Build sr.Serde with the Confluent wire-format header.
	s := sr.NewSerde(sr.Header(&sr.ConfluentHeader{}))

	// Register encode/decode functions for this schema ID + message type.
	// Index(0) marks this as the first (index 0) message in the .proto file,
	// which is required for the Confluent protobuf wire format.
	s.Register(
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
		sr.Index(0),
	)

	return &SchemaRegistry{
		serde:    s,
		msgType:  msgType,
		schemaID: ss.ID,
	}, nil
}

// Encode marshals msg and prepends the Confluent SR wire header.
func (sr_ *SchemaRegistry) Encode(msg proto.Message) ([]byte, error) {
	b, err := sr_.serde.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("serde: encode: %w", err)
	}
	return b, nil
}

// Decode strips the Confluent SR wire header and unmarshals the payload into into.
func (sr_ *SchemaRegistry) Decode(data []byte, into proto.Message) error {
	if err := sr_.serde.Decode(data, into); err != nil {
		return fmt.Errorf("serde: decode: %w", err)
	}
	return nil
}
