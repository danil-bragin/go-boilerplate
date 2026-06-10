package consume

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// EventTypeFor derives the versioned event-type wire name for the proto
// message T: "<pkg>.<Message>.v<version>", where <pkg> is the proto package
// with its trailing ".vN" version segment stripped.
//
//	EventTypeFor[*ordersv1.OrderCreated](1) == "orders.OrderCreated.v1"
//	(proto full name "orders.v1.OrderCreated")
//
// This is the SINGLE source of event-type strings in the repository:
// producers stamp outbox.Message.EventType / event-type headers with it and
// consumers register handlers via TypedFor, so the name can never drift
// between the two sides. The derivation itself is wire-frozen — changing it
// for an existing (message, version) pair is a breaking protocol change
// (pinned by TestEventTypeFor_WireCompat).
//
// When the proto package has no trailing ".vN" segment (e.g.
// "google.protobuf"), the package is used as-is; the ".v<version>" suffix
// always comes from the version parameter.
func EventTypeFor[T proto.Message](version int) string {
	var zero T
	full := string(zero.ProtoReflect().Descriptor().FullName()) // "orders.v1.OrderCreated"

	pkg, msg := "", full
	if i := strings.LastIndex(full, "."); i >= 0 {
		pkg, msg = full[:i], full[i+1:]
	}
	// Strip the proto package's version segment ("orders.v1" → "orders"):
	// the wire convention carries the version as a SUFFIX on the event name.
	if i := strings.LastIndex(pkg, "."); i >= 0 && isVersionSegment(pkg[i+1:]) {
		pkg = pkg[:i]
	}
	if pkg == "" {
		return fmt.Sprintf("%s.v%d", msg, version)
	}
	return fmt.Sprintf("%s.%s.v%d", pkg, msg, version)
}

// isVersionSegment reports whether s is a proto package version segment of
// the form "v<digits>" ("v1", "v12").
func isVersionSegment(s string) bool {
	if len(s) < 2 || s[0] != 'v' {
		return false
	}
	for _, r := range s[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// TypedFor is Typed with the event-type name derived from T via EventTypeFor:
// the handler is registered under "<pkg>.<Message>.v<version>". Prefer it
// over Typed with a hand-written string — the name then provably matches
// what producers derive for the same message and version.
func TypedFor[T proto.Message](
	version int,
	fn func(context.Context, T) error,
	onCommitted ...func(context.Context, T),
) Handler {
	return Typed(EventTypeFor[T](version), fn, onCommitted...)
}
