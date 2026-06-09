package fixtures_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"go-boilerplate/platform/testkit/fixtures"
)

// ---------------------------------------------------------------------------
// OutboxMessage
// ---------------------------------------------------------------------------

func TestOutboxMessage_Defaults(t *testing.T) {
	m := fixtures.OutboxMessage()

	assert.NotEqual(t, uuid.Nil, m.ID, "ID should default to a fresh uuid")
	assert.Equal(t, "orders.events", m.AggregateType)
	assert.Equal(t, "agg-1", m.AggregateID)
	assert.Equal(t, "Created", m.EventType)
	assert.Equal(t, []byte("{}"), m.Payload)
	assert.Equal(t, []byte("{}"), m.Headers)
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), m.CreatedAt)
}

func TestOutboxMessage_WithID(t *testing.T) {
	id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	m := fixtures.OutboxMessage(fixtures.WithID(id))
	assert.Equal(t, id, m.ID)
}

func TestOutboxMessage_WithAggregateType(t *testing.T) {
	m := fixtures.OutboxMessage(fixtures.WithAggregateType("payments.events"))
	assert.Equal(t, "payments.events", m.AggregateType)
}

func TestOutboxMessage_WithAggregateID(t *testing.T) {
	m := fixtures.OutboxMessage(fixtures.WithAggregateID("agg-99"))
	assert.Equal(t, "agg-99", m.AggregateID)
}

func TestOutboxMessage_WithEventType(t *testing.T) {
	m := fixtures.OutboxMessage(fixtures.WithEventType("Updated"))
	assert.Equal(t, "Updated", m.EventType)
}

func TestOutboxMessage_WithPayload(t *testing.T) {
	payload := []byte(`{"amount":42}`)
	m := fixtures.OutboxMessage(fixtures.WithPayload(payload))
	assert.Equal(t, payload, m.Payload)
}

func TestOutboxMessage_IndependentIDs(t *testing.T) {
	m1 := fixtures.OutboxMessage()
	m2 := fixtures.OutboxMessage()
	assert.NotEqual(t, m1.ID, m2.ID, "each call should generate a fresh ID")
}

// ---------------------------------------------------------------------------
// Principal
// ---------------------------------------------------------------------------

func TestPrincipal_Defaults(t *testing.T) {
	p := fixtures.Principal()

	assert.Equal(t, "u1", p.Subject)
	assert.Equal(t, "user", p.Username)
	assert.Equal(t, []string{"user"}, p.Roles)
}

func TestPrincipal_WithSubject(t *testing.T) {
	p := fixtures.Principal(fixtures.WithSubject("admin-42"))
	assert.Equal(t, "admin-42", p.Subject)
}

func TestPrincipal_WithUsername(t *testing.T) {
	p := fixtures.Principal(fixtures.WithUsername("alice"))
	assert.Equal(t, "alice", p.Username)
}

func TestPrincipal_WithRoles(t *testing.T) {
	p := fixtures.Principal(fixtures.WithRoles([]string{"admin", "user"}))
	assert.Equal(t, []string{"admin", "user"}, p.Roles)
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

func TestRecord_Defaults(t *testing.T) {
	r := fixtures.Record()

	assert.Equal(t, "topic", r.Topic)
	assert.Equal(t, []byte("k"), r.Key)
	assert.Equal(t, []byte("v"), r.Value)
	assert.NotNil(t, r.Headers)
	assert.Empty(t, r.Headers)
}

func TestRecord_WithTopic(t *testing.T) {
	r := fixtures.Record(fixtures.WithTopic("orders"))
	assert.Equal(t, "orders", r.Topic)
}

func TestRecord_WithKey(t *testing.T) {
	r := fixtures.Record(fixtures.WithKey([]byte("my-key")))
	assert.Equal(t, []byte("my-key"), r.Key)
}

func TestRecord_WithValue(t *testing.T) {
	r := fixtures.Record(fixtures.WithValue([]byte(`{"id":1}`)))
	assert.Equal(t, []byte(`{"id":1}`), r.Value)
}

func TestRecord_WithHeader(t *testing.T) {
	r := fixtures.Record(
		fixtures.WithHeader("content-type", "application/json"),
		fixtures.WithHeader("x-trace-id", "abc123"),
	)
	assert.Equal(t, "application/json", r.Headers["content-type"])
	assert.Equal(t, "abc123", r.Headers["x-trace-id"])
}

func TestRecord_HeadersIndependent(t *testing.T) {
	r1 := fixtures.Record(fixtures.WithHeader("k", "v1"))
	r2 := fixtures.Record(fixtures.WithHeader("k", "v2"))
	assert.Equal(t, "v1", r1.Headers["k"])
	assert.Equal(t, "v2", r2.Headers["k"])
}
