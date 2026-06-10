// Package msgctx carries message-chain metadata (correlation and causation
// ids) through context, linking every event a handler emits back to the
// command that started the chain:
//
//   - correlation-id: constant across one whole chain — equal to the
//     message-id of the original command (the chain root). Seeded by the
//     edge producer (e.g. the gateway sets correlation-id = order id on the
//     CreateOrderCommand record) and propagated verbatim by every consumer.
//   - causation-id: the message-id of the DIRECT parent message — the record
//     whose handler emitted this event. Changes at every hop.
//
// The consume package installs both values before invoking a typed handler;
// outbox.Repository.Enqueue stamps them onto outgoing messages automatically
// (explicit Message.Headers values win). Services therefore get full chain
// lineage without touching either id.
package msgctx

import "context"

// Kafka/outbox header names for the chain metadata.
const (
	// HeaderCorrelationID is the header carrying the chain-constant
	// correlation id (== the root command's message id).
	HeaderCorrelationID = "correlation-id"
	// HeaderCausationID is the header carrying the parent message id.
	HeaderCausationID = "causation-id"
)

// Context keys use the empty-struct style shared across the repo
// (auth, log, pg): distinct types cost zero bytes and cannot collide.
type (
	correlationIDKey   struct{}
	parentMessageIDKey struct{}
)

// WithCorrelationID returns a ctx carrying the chain correlation id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationID returns the chain correlation id, or "" when none is set.
func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(correlationIDKey{}).(string)
	return v
}

// WithParentMessageID returns a ctx carrying the message id of the record
// currently being processed — the causation parent for anything the handler
// emits.
func WithParentMessageID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, parentMessageIDKey{}, id)
}

// ParentMessageID returns the current message id (causation parent), or ""
// when none is set (e.g. outside a consumer).
func ParentMessageID(ctx context.Context) string {
	v, _ := ctx.Value(parentMessageIDKey{}).(string)
	return v
}
