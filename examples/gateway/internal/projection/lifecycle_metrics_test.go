package projection_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"

	"go-boilerplate/examples/gateway/internal/migrations"
	"go-boilerplate/examples/gateway/internal/projection"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// metricsReader installs a manual-reader SDK provider as the otel global,
// exactly once per test binary.
var (
	metricsReaderOnce sync.Once
	metricsReader     *sdkmetric.ManualReader
)

func manualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	metricsReaderOnce.Do(func() {
		metricsReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricsReader)))
	})
	return metricsReader
}

// lifecycleCount returns the orders.lifecycle.duration datapoint count for
// the given terminal_status label.
func lifecycleCount(rm *metricdata.ResourceMetrics, terminalStatus string) uint64 {
	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "orders.lifecycle.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if string(kv.Key) == "terminal_status" && kv.Value.AsString() == terminalStatus {
						total += dp.Count
					}
				}
			}
		}
	}
	return total
}

// TestProjection_LifecycleDurationMetric (integration): the projection
// observes orders.lifecycle.duration {terminal_status} exactly once per
// APPLIED terminal write — a replayed terminal event (0-row upsert, first
// terminal wins) must NOT produce a second sample, and each terminal status
// labels its own series.
func TestProjection_LifecycleDurationMetric(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	reader := manualReader(t)
	ctx := context.Background()

	dsn := pgtest.SharedDSN(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "sql"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	broker := fakes.NewBroker()
	handler := projection.NewHandler(pool, slog.Default(), nil, nil, consume.WithoutInbox())
	broker.Subscribe("orders.events", handler)
	broker.Subscribe("payments.events", handler)

	publish := func(topic, eventType string, msg proto.Message) {
		t.Helper()
		payload, err := proto.Marshal(msg)
		require.NoError(t, err)
		require.NoError(t, broker.Publish(ctx, outbox.Message{
			ID:        uuid.New(),
			Topic:     topic,
			EventType: eventType,
			Payload:   payload,
		}))
	}

	collect := func() metricdata.ResourceMetrics {
		t.Helper()
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(ctx, &rm))
		return rm
	}

	// created → paid: exactly one {terminal_status=paid} sample.
	paidOrder := uuid.New().String()
	publish("orders.events", projection.OrderCreatedEventType, &ordersv1.OrderCreated{
		OrderId: paidOrder, CustomerId: "c-1", AmountCents: 100, Currency: "USD",
	})
	rm := collect()
	require.Zero(t, lifecycleCount(&rm, "paid"), "non-terminal writes must not observe lifecycle duration")

	publish("payments.events", projection.PaymentProcessedEventType, &ordersv1.PaymentProcessed{
		OrderId: paidOrder,
	})
	rm = collect()
	require.Equal(t, uint64(1), lifecycleCount(&rm, "paid"), "terminal write must observe exactly one sample")

	// Replayed PaymentProcessed: upsert applies 0 rows → NO second sample.
	publish("payments.events", projection.PaymentProcessedEventType, &ordersv1.PaymentProcessed{
		OrderId: paidOrder,
	})
	rm = collect()
	require.Equal(t, uint64(1), lifecycleCount(&rm, "paid"), "replayed terminal event must not double-observe")

	// Second order failing: labels its own terminal_status series.
	failedOrder := uuid.New().String()
	publish("orders.events", projection.OrderCreatedEventType, &ordersv1.OrderCreated{
		OrderId: failedOrder, CustomerId: "c-2", AmountCents: 200, Currency: "USD",
	})
	publish("payments.events", projection.PaymentFailedEventType, &ordersv1.PaymentFailed{
		OrderId: failedOrder, Reason: "card declined",
	})
	rm = collect()
	require.Equal(t, uint64(1), lifecycleCount(&rm, "payment_failed"))
	require.Equal(t, uint64(1), lifecycleCount(&rm, "paid"), "paid series must be untouched by the failed order")

	// Reordered: the terminal event arrives BEFORE OrderCreated. The upsert's
	// INSERT arm creates a placeholder row whose created_at is the row
	// insertion time — the real creation time is unknown, so a lifecycle
	// sample would lie compliant (≈0s) and bias the SLO-2 good leg. The
	// projection must record NO sample for an inserted (placeholder) row.
	reorderedOrder := uuid.New().String()
	publish("payments.events", projection.PaymentProcessedEventType, &ordersv1.PaymentProcessed{
		OrderId: reorderedOrder,
	})
	rm = collect()
	require.Equal(t, uint64(1), lifecycleCount(&rm, "paid"),
		"terminal-before-created reorder must not observe a lifecycle sample (creation time unknown)")
}
