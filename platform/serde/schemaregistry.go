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
//	The schema text for PROTOBUF type is derived programmatically from the
//	proto.Message's FileDescriptor (package + message fields), producing a valid
//	proto3 source string that Redpanda / Confluent SR accepts.
//
//	Protobuf message-index: for a top-level message that is the first (or only)
//	message in its .proto file, the Confluent index is [0], encoded as a single
//	zero-byte varint pair per the wire-format spec.

import (
	"context"
	"fmt"
	"strings"

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
func NewSchemaRegistry(srURL string, subject string, msg proto.Message) (*SchemaRegistry, error) {
	// Build SR client.
	client, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		return nil, fmt.Errorf("serde: sr client: %w", err)
	}

	// Derive proto schema text from the message's file descriptor.
	schemaText := protoSchemaText(msg.ProtoReflect().Descriptor())

	// Register (or look up) the schema, obtaining the globally-unique schema ID.
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

// protoSchemaText derives a minimal but valid proto3 schema text from a
// MessageDescriptor. It produces a schema that Confluent SR / Redpanda accepts
// for the TypeProtobuf schema type, covering the common scalar field kinds.
func protoSchemaText(desc protoreflect.MessageDescriptor) string {
	var sb strings.Builder

	fileDesc := desc.ParentFile()
	pkg := string(fileDesc.Package())

	sb.WriteString("syntax = \"proto3\";\n")
	if pkg != "" {
		sb.WriteString("package ")
		sb.WriteString(pkg)
		sb.WriteString(";\n")
	}

	writeMessage(&sb, desc, "")
	return sb.String()
}

func writeMessage(sb *strings.Builder, desc protoreflect.MessageDescriptor, indent string) {
	sb.WriteString(indent + "message ")
	sb.WriteString(string(desc.Name()))
	sb.WriteString(" {\n")

	fields := desc.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		typeName := kindToProtoType(f)
		fieldName := string(f.Name())
		num := f.Number()

		if f.IsList() {
			fmt.Fprintf(sb, "%s  repeated %s %s = %d;\n", indent, typeName, fieldName, num)
		} else {
			fmt.Fprintf(sb, "%s  %s %s = %d;\n", indent, typeName, fieldName, num)
		}
	}

	// Nested messages (e.g. map entries, embedded types).
	nested := desc.Messages()
	for i := range nested.Len() {
		n := nested.Get(i)
		if n.IsMapEntry() {
			continue // map entries are handled by the containing field's type name
		}
		writeMessage(sb, n, indent+"  ")
	}

	sb.WriteString(indent + "}\n")
}

// kindToProtoType maps a FieldDescriptor's Kind to its proto3 type keyword.
func kindToProtoType(f protoreflect.FieldDescriptor) string {
	switch f.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.EnumKind:
		return string(f.Enum().Name())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind:
		return "int32"
	case protoreflect.Uint32Kind:
		return "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind:
		return "int64"
	case protoreflect.Uint64Kind:
		return "uint64"
	case protoreflect.Sfixed32Kind, protoreflect.Fixed32Kind:
		return "fixed32"
	case protoreflect.Sfixed64Kind, protoreflect.Fixed64Kind:
		return "fixed64"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if f.IsMap() {
			kf := f.MapKey()
			vf := f.MapValue()
			return fmt.Sprintf("map<%s, %s>", kindToProtoType(kf), kindToProtoType(vf))
		}
		return string(f.Message().Name())
	default:
		return "bytes"
	}
}
