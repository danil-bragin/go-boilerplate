package projection_test

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewHandler_SkipsUnknownEventTypes pins the projection's forward
// compatibility through the REAL handler via fakes.Broker: an event type the
// projection does not know (a newer producer) must be skipped — nil error so
// the offset commits and the consumer keeps flowing. The nil pool proves the
// skip happens before any inbox/database work.
//
// (Terminal-status precedence — pending < created < terminal, first terminal
// wins — lives in the upsert SQL and is covered by the projection integration
// suite in examples/gateway/projection_app_test.go; it is deliberately NOT
// re-modelled here as a Go function that the SQL would not use.)
func TestNewHandler_SkipsUnknownEventTypes(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()
	handler := projection.NewHandler(pg.WrapPool(nil), slog.Default(), nil, nil, consume.WithoutInbox())
	broker.Subscribe("orders.events", handler)
	broker.Subscribe("payments.events", handler)

	for _, unknown := range []struct{ topic, eventType string }{
		{"orders.events", "orders.OrderShipped.v1"},      // future producer
		{"orders.events", "orders.OrderCreated.v2"},      // version bump
		{"payments.events", "orders.PaymentRefunded.v1"}, // new branch
		{"payments.events", ""},                          // missing header entirely
	} {
		err := broker.Publish(context.Background(), outbox.Message{
			ID:        uuid.New(),
			Topic:     unknown.topic,
			EventType: unknown.eventType,
			Payload:   []byte("opaque"),
		})
		require.NoError(t, err, "unknown event type %q must be skipped, not errored (would stall the partition)", unknown.eventType)
	}
}

// TestNewHandler_RoutesAllFourEventTypes pins the projection's event-type
// routing table: each of the four consumed types must reach ITS handler (and
// fail decode loudly on a garbage payload — proving dispatch happened rather
// than a silent skip). Catches the classic copy-paste bug of registering a
// new event type under the wrong handler or forgetting it entirely.
func TestNewHandler_RoutesAllFourEventTypes(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()
	handler := projection.NewHandler(pg.WrapPool(nil), slog.Default(), nil, nil, consume.WithoutInbox())
	broker.Subscribe("orders.events", handler)
	broker.Subscribe("payments.events", handler)

	for _, known := range []struct{ topic, eventType string }{
		{"orders.events", projection.OrderCreatedEventType},
		{"payments.events", projection.PaymentProcessedEventType},
		{"payments.events", projection.PaymentFailedEventType},
		{"orders.events", projection.OrderPaymentTimedOutEventType},
	} {
		err := broker.Publish(context.Background(), outbox.Message{
			ID:        uuid.New(),
			Topic:     known.topic,
			EventType: known.eventType,
			Payload:   []byte{0xff, 0xff, 0xff, 0xff}, // invalid proto wire format
		})
		require.Error(t, err, "known type %q must be dispatched (decode error expected on garbage payload)", known.eventType)
		assert.ErrorContains(t, err, known.eventType, "the error must identify the failing event type")
	}
}
