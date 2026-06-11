package projection_test

// Consumer half of the projection contracts (-short, no Docker): each
// canonical fixture from examples/e2e/contract rides the REAL outboxkafka
// record build through fakes.Broker into the REAL projection handler
// (NewHandlerWithStore + consume.WithoutInbox — decode, event-type routing
// and field mapping are the production code paths; only the SQL exec is a
// recording fake). The producer halves (orders' / payments' contract_tests)
// pin that the services emit exactly these fixtures.

import (
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"go-boilerplate/examples/e2e/contract"
	gateway "go-boilerplate/examples/gateway"
	"go-boilerplate/examples/gateway/internal/projection"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// recordingStore captures every orders_read write the projection issues.
// Terminal marks report Inserted=true (placeholder-row shape) so the
// lifecycle metric path is skipped — metrics are not under contract here.
type recordingStore struct {
	upserts  []storegen.UpsertOrderCreatedParams
	paid     []uuid.UUID
	failed   []uuid.UUID
	timedOut []uuid.UUID
}

func (s *recordingStore) UpsertOrderCreated(_ context.Context, arg storegen.UpsertOrderCreatedParams) error {
	s.upserts = append(s.upserts, arg)
	return nil
}

func (s *recordingStore) MarkPaid(_ context.Context, orderID uuid.UUID) (storegen.MarkPaidRow, error) {
	s.paid = append(s.paid, orderID)
	return storegen.MarkPaidRow{Inserted: true}, nil
}

func (s *recordingStore) MarkPaymentFailed(_ context.Context, orderID uuid.UUID) (storegen.MarkPaymentFailedRow, error) {
	s.failed = append(s.failed, orderID)
	return storegen.MarkPaymentFailedRow{Inserted: true}, nil
}

func (s *recordingStore) MarkPaymentTimeout(_ context.Context, orderID uuid.UUID) (storegen.MarkPaymentTimeoutRow, error) {
	s.timedOut = append(s.timedOut, orderID)
	return storegen.MarkPaymentTimeoutRow{Inserted: true}, nil
}

// contractHarness wires the real handler over the recording store and a
// notify capture, subscribed on both contract topics.
func contractHarness(t *testing.T) (*fakes.Broker, *recordingStore, *[]string) {
	t.Helper()
	store := &recordingStore{}
	var notified []string
	handler := projection.NewHandlerWithStore(
		nil,
		func(context.Context) projection.Store { return store },
		slog.New(slog.DiscardHandler), nil,
		func(_ context.Context, orderID string) { notified = append(notified, orderID) },
		consume.WithoutInbox(),
	)
	broker := fakes.NewBroker()
	broker.Subscribe(contract.OrdersEventsTopic, handler)
	broker.Subscribe(contract.PaymentsEventsTopic, handler)
	return broker, store, &notified
}

func produce(t *testing.T, broker *fakes.Broker, msg outbox.Message) {
	t.Helper()
	require.NoError(t, broker.Produce(context.Background(), contract.WireRecord(t, msg)))
}

// TestContract_OrderCreatedProjection asserts the field-level mapping from
// the canonical OrderCreated wire event to the orders_read upsert: id,
// customer, amount and currency — and the post-commit SSE notify.
func TestContract_OrderCreatedProjection(t *testing.T) {
	t.Parallel()
	broker, store, notified := contractHarness(t)
	orderID := uuid.New()

	produce(t, broker, contract.OrderCreated(t, orderID.String(), "cust-42", 123_456, "EUR"))

	require.Len(t, store.upserts, 1, "one wire event must issue exactly one upsert")
	assert.Equal(t, storegen.UpsertOrderCreatedParams{
		OrderID:     orderID,
		CustomerID:  "cust-42",
		AmountCents: 123_456,
		Currency:    "EUR",
	}, store.upserts[0], "OrderCreated → orders_read field mapping")
	assert.Equal(t, []string{orderID.String()}, *notified,
		"post-commit notify must fire with the order id (SSE liveness)")
}

// TestContract_TerminalEventsProjection asserts each payments/orders
// terminal event routes to ITS status write with the right order id.
func TestContract_TerminalEventsProjection(t *testing.T) {
	t.Parallel()
	broker, store, notified := contractHarness(t)
	paidID, failedID, timeoutID := uuid.New(), uuid.New(), uuid.New()
	deadline := time.Date(2026, 6, 11, 8, 9, 0, 0, time.UTC)

	produce(t, broker, contract.PaymentProcessed(t, paidID.String(), uuid.NewString()))
	produce(t, broker, contract.PaymentFailed(t, failedID.String(), "declined", deadline))
	produce(t, broker, contract.OrderPaymentTimedOut(t, timeoutID.String(), deadline))

	assert.Equal(t, []uuid.UUID{paidID}, store.paid, "PaymentProcessed → MarkPaid")
	assert.Equal(t, []uuid.UUID{failedID}, store.failed, "PaymentFailed → MarkPaymentFailed")
	assert.Equal(t, []uuid.UUID{timeoutID}, store.timedOut, "OrderPaymentTimedOut → MarkPaymentTimeout")
	assert.Empty(t, store.upserts, "terminal events must never hit the OrderCreated upsert")
	assert.Equal(t, []string{paidID.String(), failedID.String(), timeoutID.String()}, *notified,
		"every committed status change must notify SSE")
}

// TestContract_TopicDefaults pins the gateway's PRODUCTION topic defaults
// (the env tags on gateway.Config) against the contract constants the
// fixtures ride on — a one-sided default rename silently detaches the
// projection from the producers.
func TestContract_TopicDefaults(t *testing.T) {
	t.Parallel()
	cfgType := reflect.TypeOf(gateway.Config{})
	for field, want := range map[string]string{
		"OrdersEventsTopic":   contract.OrdersEventsTopic,
		"PaymentsEventsTopic": contract.PaymentsEventsTopic,
	} {
		f, ok := cfgType.FieldByName(field)
		require.True(t, ok, "gateway.Config.%s field", field)
		assert.Equal(t, want, f.Tag.Get("envDefault"), "gateway.Config.%s default", field)
	}
}
