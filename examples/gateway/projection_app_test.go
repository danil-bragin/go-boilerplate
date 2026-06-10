package gateway_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	gateway "go-boilerplate/examples/gateway"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestProjectionApp_StandaloneSmoke verifies the standalone projection binary
// wiring (gateway.NewProjectionApp, used by examples/gateway/cmd/projection):
// it constructs, starts, consumes an OrderCreated event into orders_read,
// and stops cleanly — all without a public HTTP server.
func TestProjectionApp_StandaloneSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	configureTopics(t)
	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "projection-smoke-"+uuid.New().String())
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	app, err := gateway.NewProjectionApp(ctx)
	require.NoError(t, err, "standalone projection wiring must construct")
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = app.Stop(stopCtx)
	})

	// Produce one OrderCreated and assert the read-model row appears.
	orderID := uuid.New().String()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: orderID, CustomerId: "c-split", AmountCents: 300, Currency: "USD",
	})
	require.NoError(t, err)
	produceRaw(t, broker, topicOrdersEvents, orderID, payload, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": orderID,
	})

	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	require.Eventually(t, func() bool {
		var status string
		err := pool.Reader().QueryRow(context.Background(),
			`select status from orders_read where order_id = $1`, orderID).Scan(&status)
		return err == nil && status == "created"
	}, 30*time.Second, 300*time.Millisecond, "standalone projection must apply OrderCreated to orders_read")
}

// TestGateway_EmbeddedProjectionDisabled verifies the split seam on the
// gateway side: with GATEWAY_EMBEDDED_PROJECTION=false the gateway serves
// HTTP but registers NO projection consumer — events are not applied.
func TestGateway_EmbeddedProjectionDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("GATEWAY_EMBEDDED_PROJECTION", "false")
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()
	payload, err := proto.Marshal(&ordersv1.OrderCreated{
		OrderId: orderID, CustomerId: "c-noproj", AmountCents: 100, Currency: "USD",
	})
	require.NoError(t, err)
	produceRaw(t, broker, topicOrdersEvents, orderID, payload, map[string]string{
		"event-type": "orders.OrderCreated.v1",
		"message-id": orderID,
	})

	// Give a generous consumption window: the row must NOT appear because no
	// projection consumer runs inside this gateway.
	time.Sleep(5 * time.Second)
	require.Equal(t, "", getStatus(t, baseURL, orderID),
		"with embedded projection disabled the gateway must not apply events")
}

// produceRaw publishes a raw record with explicit headers.
func produceRaw(t *testing.T, broker, topic, key string, value []byte, headers map[string]string) {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "projection-smoke-producer-" + uuid.New().String(),
	})
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})
	require.NoError(t, p.Produce(context.Background(), kafka.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
	}))
}
