// Package fixtures provides builder functions with functional options for
// constructing platform types in tests. Every builder has sane defaults so
// callers only need to override the fields they care about.
package fixtures

import (
	"time"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
)

// fixedTime is the default CreatedAt used by OutboxMessage.
var fixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// OutboxMessage
// ---------------------------------------------------------------------------

// OutboxMessageOpt is a functional option for outbox.Message.
type OutboxMessageOpt func(*outbox.Message)

// WithID overrides the message ID.
func WithID(id uuid.UUID) OutboxMessageOpt {
	return func(m *outbox.Message) { m.ID = id }
}

// WithAggregateType overrides the AggregateType field.
func WithAggregateType(t string) OutboxMessageOpt {
	return func(m *outbox.Message) { m.AggregateType = t }
}

// WithAggregateID overrides the AggregateID field.
func WithAggregateID(id string) OutboxMessageOpt {
	return func(m *outbox.Message) { m.AggregateID = id }
}

// WithEventType overrides the EventType field.
func WithEventType(et string) OutboxMessageOpt {
	return func(m *outbox.Message) { m.EventType = et }
}

// WithPayload overrides the Payload field.
func WithPayload(p []byte) OutboxMessageOpt {
	return func(m *outbox.Message) { m.Payload = p }
}

// OutboxMessage returns an outbox.Message with sensible defaults. Apply
// functional options to override individual fields.
func OutboxMessage(opts ...OutboxMessageOpt) outbox.Message {
	m := outbox.Message{
		ID:            uuid.New(),
		AggregateType: "orders.events",
		AggregateID:   "agg-1",
		EventType:     "Created",
		Payload:       []byte("{}"),
		Headers:       []byte("{}"),
		CreatedAt:     fixedTime,
	}
	for _, o := range opts {
		o(&m)
	}
	return m
}

// ---------------------------------------------------------------------------
// Principal
// ---------------------------------------------------------------------------

// PrincipalOpt is a functional option for auth.Principal.
type PrincipalOpt func(*auth.Principal)

// WithSubject overrides the Subject field.
func WithSubject(s string) PrincipalOpt {
	return func(p *auth.Principal) { p.Subject = s }
}

// WithUsername overrides the Username field.
func WithUsername(u string) PrincipalOpt {
	return func(p *auth.Principal) { p.Username = u }
}

// WithRoles overrides the Roles slice.
func WithRoles(roles []string) PrincipalOpt {
	return func(p *auth.Principal) { p.Roles = roles }
}

// Principal returns an auth.Principal with sensible defaults. Apply functional
// options to override individual fields.
func Principal(opts ...PrincipalOpt) auth.Principal {
	p := auth.Principal{
		Subject:  "u1",
		Username: "user",
		Roles:    []string{"user"},
	}
	for _, o := range opts {
		o(&p)
	}
	return p
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

// RecordOpt is a functional option for kafka.Record.
type RecordOpt func(*kafka.Record)

// WithTopic overrides the Topic field.
func WithTopic(t string) RecordOpt {
	return func(r *kafka.Record) { r.Topic = t }
}

// WithKey overrides the Key field.
func WithKey(k []byte) RecordOpt {
	return func(r *kafka.Record) { r.Key = k }
}

// WithValue overrides the Value field.
func WithValue(v []byte) RecordOpt {
	return func(r *kafka.Record) { r.Value = v }
}

// WithHeader adds or overwrites a single header key/value pair.
func WithHeader(k, v string) RecordOpt {
	return func(r *kafka.Record) { r.Headers[k] = v }
}

// Record returns a kafka.Record with sensible defaults. Apply functional
// options to override individual fields.
func Record(opts ...RecordOpt) kafka.Record {
	r := kafka.Record{
		Topic:   "topic",
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: make(map[string]string),
	}
	for _, o := range opts {
		o(&r)
	}
	return r
}
