// Package serde encodes/decodes protobuf event payloads, optionally with a
// Confluent Schema Registry wire format.
package serde

import "google.golang.org/protobuf/proto"

// Serializer encodes a proto message to bytes.
type Serializer interface {
	Encode(msg proto.Message) ([]byte, error)
}

// Deserializer decodes bytes into the given proto message.
type Deserializer interface {
	Decode(data []byte, into proto.Message) error
}
