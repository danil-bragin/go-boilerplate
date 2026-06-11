package outboxkafka

// Internal (white-box) tests for the malformed-headers drop path: the drop
// stays (head-of-line-block prevention) but is no longer silent — a WARN with
// the outbox row id is emitted so a poison row is visible in logs.

import (
	"bytes"
	"log/slog"
	"testing"

	"go-boilerplate/platform/messaging/outbox"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageToRecord_MalformedHeadersWarnsWithRowID(t *testing.T) {
	var buf bytes.Buffer
	p := New(nil, WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))

	id := uuid.New()
	msg := outbox.Message{
		ID:            id,
		AggregateID:   "agg-1",
		AggregateType: "order",
		EventType:     "orders.OrderCreated.v1",
		Topic:         "orders.events",
		Payload:       []byte("payload"),
		Headers:       []byte(`not-a-json-object`),
	}

	rec, err := p.messageToRecord(msg)
	require.NoError(t, err, "a malformed-header row must still publish (no head-of-line block)")

	// Standard headers only — the custom ones were dropped.
	assert.Equal(t, "orders.OrderCreated.v1", rec.Headers["event-type"])
	assert.Equal(t, id.String(), rec.Headers["message-id"])
	assert.Equal(t, "order", rec.Headers["aggregate-type"])
	assert.Len(t, rec.Headers, 3)

	logged := buf.String()
	assert.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(t, logged, id.String(), "WARN must carry the outbox row id")
	assert.Contains(t, logged, "outboxkafka: malformed Message.Headers JSON")
}

func TestMessageToRecord_ValidHeadersNoWarn(t *testing.T) {
	var buf bytes.Buffer
	p := New(nil, WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))

	msg := outbox.Message{
		ID:            uuid.New(),
		AggregateID:   "agg-1",
		AggregateType: "order",
		EventType:     "orders.OrderCreated.v1",
		Payload:       []byte("payload"),
		Headers:       []byte(`{"k":"v"}`),
	}

	rec, err := p.messageToRecord(msg)
	require.NoError(t, err)
	assert.Equal(t, "v", rec.Headers["k"])
	assert.Empty(t, buf.String(), "well-formed headers must not WARN")
}
