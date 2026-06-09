package serde

import "google.golang.org/protobuf/proto"

// Protobuf is a plain protobuf serializer/deserializer with no schema-registry
// wire envelope. It implements both Serializer and Deserializer.
type Protobuf struct{}

// NewProtobuf returns a Protobuf serde that uses proto.Marshal / proto.Unmarshal
// directly, with no Schema Registry framing.
func NewProtobuf() *Protobuf {
	return &Protobuf{}
}

// Encode marshals msg to protobuf wire format.
func (p *Protobuf) Encode(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

// Decode unmarshals data into the given proto message.
func (p *Protobuf) Decode(data []byte, into proto.Message) error {
	return proto.Unmarshal(data, into)
}
