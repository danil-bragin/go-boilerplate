package transport_test

import (
	"context"
	"testing"

	"go-boilerplate/examples/orders/internal/app"
	"go-boilerplate/examples/orders/internal/transport"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestNewCommandHandler_DecodesAndDispatches drives the REAL command-consumer
// pipeline (header dispatch → proto decode → command mapping) through
// fakes.Broker — no Docker. consume.WithoutInbox is the documented test-only
// escape hatch (production keeps inbox dedup).
func TestNewCommandHandler_DecodesAndDispatches(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	var got []app.CreateOrder
	handler := transport.NewCommandHandler(nil, func(_ context.Context, cmd app.CreateOrder) (app.CreateOrderResult, error) {
		got = append(got, cmd)
		return app.CreateOrderResult{OrderID: cmd.OrderID}, nil
	}, consume.WithoutInbox())
	broker.Subscribe("orders.commands", handler)

	orderID := uuid.NewString()
	payload, err := proto.Marshal(&ordersv1.CreateOrderCommand{
		OrderId: orderID, CustomerId: "cust-9", AmountCents: 1500, Currency: "USD",
	})
	require.NoError(t, err)
	require.NoError(t, broker.Publish(context.Background(), outbox.Message{
		ID:          uuid.New(),
		Topic:       "orders.commands",
		AggregateID: orderID,
		EventType:   transport.CommandEventType,
		Payload:     payload,
	}))

	require.Len(t, got, 1)
	assert.Equal(t, app.CreateOrder{OrderID: orderID, CustomerID: "cust-9", AmountCents: 1500, Currency: "USD"}, got[0],
		"every command field must survive decode → command mapping")

	// Unknown event types on the same topic are skipped, not dispatched.
	require.NoError(t, broker.Publish(context.Background(), outbox.Message{
		ID: uuid.New(), Topic: "orders.commands", EventType: "orders.CancelOrderCommand.v9",
	}))
	assert.Len(t, got, 1)
}

// TestNewCommandHandler_CorrelationLineage pins chain lineage at the edge of
// the choreography: the consumer must expose the incoming correlation id and
// make THIS message the causation parent of whatever the handler emits.
func TestNewCommandHandler_CorrelationLineage(t *testing.T) {
	t.Parallel()
	broker := fakes.NewBroker()

	var gotCorr, gotParent string
	handler := transport.NewCommandHandler(nil, func(ctx context.Context, _ app.CreateOrder) (app.CreateOrderResult, error) {
		gotCorr = msgctx.CorrelationID(ctx)
		gotParent = msgctx.ParentMessageID(ctx)
		return app.CreateOrderResult{}, nil
	}, consume.WithoutInbox())
	broker.Subscribe("orders.commands", handler)

	msgID := uuid.New()
	payload, err := proto.Marshal(&ordersv1.CreateOrderCommand{OrderId: uuid.NewString()})
	require.NoError(t, err)
	require.NoError(t, broker.Publish(context.Background(), outbox.Message{
		ID:        msgID,
		Topic:     "orders.commands",
		EventType: transport.CommandEventType,
		Payload:   payload,
		Headers:   []byte(`{"correlation-id":"edge-request-1"}`),
	}))

	assert.Equal(t, "edge-request-1", gotCorr, "correlation id from the edge must reach the handler ctx")
	assert.Equal(t, msgID.String(), gotParent, "this message becomes the causation parent")
}
