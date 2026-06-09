// Package proto embeds the repository's .proto contract sources so runtime
// components (e.g. the Schema Registry serde in platform/messaging/serde) can
// register the EXACT source text that buf compiled — no schema printing or
// reconstruction, hence zero drift between the registered schema and the
// generated Go types.
package proto

import "embed"

// FS contains every .proto file under proto/, keyed by its buf module path
// (e.g. "orders/v1/orders.proto"). This is the same path returned by a
// generated message's protoreflect FileDescriptor.Path(), so callers can map
// a proto.Message to its source text without any extra configuration.
//
//go:embed */*/*.proto
var FS embed.FS
