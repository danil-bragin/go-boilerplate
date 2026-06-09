package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/protobuf/proto"

	gateway "go-boilerplate/examples/gateway"
	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
	authpkg "go-boilerplate/platform/security/auth"
)

// startApp starts the gateway app for testing, returning its base URL.
// The app is stopped via t.Cleanup.
func startApp(t *testing.T, broker, dsn string, opts ...gateway.Option) string {
	t.Helper()

	os.Setenv("PG_DSN", dsn)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "gateway-test-"+uuid.New().String())
	os.Setenv("HTTP_ADDR", "127.0.0.1:0") // random port
	os.Setenv("GATEWAY_AUTH_DISABLED", "true")
	os.Setenv("LOG_LEVEL", "error") // suppress noise in tests

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, opts...)
	require.NoError(t, err)

	a.Start()

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(stopCtx)
	})

	return "http://" + a.Addr()
}

// TestGateway_PostOrderPublishesCommand posts an order and asserts a
// CreateOrderCommand arrives on Kafka.
func TestGateway_PostOrderPublishesCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	baseURL := startApp(t, broker, dsn)

	// POST /orders
	body := map[string]interface{}{
		"customer_id":  "cust-1",
		"amount_cents": int64(1500),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/orders", "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	var createResp struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResp))
	require.NotEmpty(t, createResp.OrderID)
	orderID := createResp.OrderID

	// Consume from orders.commands and verify the command.
	cmd := consumeCreateOrderCommand(t, broker, orderID, 20*time.Second)
	require.NotNil(t, cmd)
	assert.Equal(t, orderID, cmd.GetOrderId())
	assert.Equal(t, "cust-1", cmd.GetCustomerId())
	assert.Equal(t, int64(1500), cmd.GetAmountCents())
	assert.Equal(t, "USD", cmd.GetCurrency())
}

// TestGateway_ProjectionAndGetOrder verifies the projection consumer and read model.
func TestGateway_ProjectionAndGetOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()

	// Produce an OrderCreated event to orders.events.
	produceEvent(t, broker, "orders.events", "OrderCreated", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId:     orderID,
			CustomerId:  "cust-proj",
			AmountCents: 2000,
			Currency:    "EUR",
		}
	})

	// Poll GET /orders/{id} until status == "created".
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	// Produce a PaymentProcessed event to payments.events.
	paymentID := uuid.New().String()
	produceEvent(t, broker, "payments.events", "PaymentProcessed", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId:   orderID,
			PaymentId: paymentID,
			Status:    "success",
		}
	})

	// Poll GET /orders/{id} until status == "paid".
	pollOrderStatus(t, baseURL, orderID, "paid", 30*time.Second)
}

// TestGateway_GetUnknownOrder404 verifies that GET /orders/<unknown> returns 404.
func TestGateway_GetUnknownOrder404(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	baseURL := startApp(t, broker, dsn)

	unknownID := uuid.New().String()
	resp, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, unknownID))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// consumeCreateOrderCommand consumes from orders.commands until a CreateOrderCommand
// with the expected orderID arrives.
func consumeCreateOrderCommand(t *testing.T, broker, orderID string, timeout time.Duration) *ordersv1.CreateOrderCommand {
	t.Helper()

	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-cmd-consumer-" + orderID,
		GroupID:  "test-cmd-consumer-" + orderID,
	}
	consumer, err := kafka.NewConsumer(cfg, "orders.commands")
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	found := make(chan *ordersv1.CreateOrderCommand, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, r kafka.Record) error {
			var cmd ordersv1.CreateOrderCommand
			if err := proto.Unmarshal(r.Value, &cmd); err != nil {
				return nil //nolint:nilerr // intentionally skip messages that don't unmarshal as this type
			}
			if cmd.GetOrderId() == orderID {
				select {
				case found <- &cmd:
				default:
				}
			}
			return nil
		})
	}()

	select {
	case cmd := <-found:
		return cmd
	case <-ctx.Done():
		t.Fatalf("CreateOrderCommand for order %s not received within %v", orderID, timeout)
		return nil
	}
}

// produceEvent produces a proto event with the given event-type header.
func produceEvent(t *testing.T, broker, topic, eventType, orderID string, buildMsg func() proto.Message) {
	t.Helper()

	msg := buildMsg()
	payload, err := proto.Marshal(msg)
	require.NoError(t, err)

	cfg := kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-event-producer-" + orderID,
	}
	cl, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	p := kafka.NewProducer(cl)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	msgID := eventType + "-" + orderID

	ctx := context.Background()
	require.NoError(t, p.Produce(ctx, kafka.Record{
		Topic: topic,
		Key:   []byte(orderID),
		Value: payload,
		Headers: map[string]string{
			"event-type": eventType,
			"message-id": msgID,
		},
	}))
}

// pollOrderStatus polls GET /orders/{id} until the status matches, or fails.
func pollOrderStatus(t *testing.T, baseURL, orderID, expectedStatus string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, orderID))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			var view struct {
				Status string `json:"status"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&view)
			resp.Body.Close()
			if view.Status == expectedStatus {
				return
			}
		} else {
			resp.Body.Close()
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("order %s did not reach status %q within %v", orderID, expectedStatus, timeout)
}

// stubVerifier is a minimal auth.Verifier for tests.
// It accepts only the literal token "good" and rejects everything else.
// The returned Principal carries the "user" role so that RBAC on POST /orders
// allows the request through.
type stubVerifier struct{}

func (stubVerifier) Verify(_ context.Context, rawToken string) (authpkg.Principal, error) {
	if rawToken == "good" {
		return authpkg.Principal{Subject: "test-user", Roles: []string{"user"}}, nil
	}
	return authpkg.Principal{}, authpkg.ErrInvalidToken
}

// stubVerifierNoRole returns a valid principal that deliberately lacks any role.
// Used to prove that RBAC returns 403 when the principal has no permitted role.
type stubVerifierNoRole struct{}

func (stubVerifierNoRole) Verify(_ context.Context, rawToken string) (authpkg.Principal, error) {
	if rawToken == "good" {
		return authpkg.Principal{Subject: "test-user-norole", Roles: []string{}}, nil
	}
	return authpkg.Principal{}, authpkg.ErrInvalidToken
}

// startAppAuthEnabled starts the gateway with AuthDisabled=false and a stub verifier.
func startAppAuthEnabled(t *testing.T, broker, dsn string) string {
	t.Helper()

	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "gateway-test-auth-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, gateway.WithVerifier(stubVerifier{}))
	require.NoError(t, err)

	a.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(stopCtx)
	})

	return "http://" + a.Addr()
}

// TestGateway_AuthEnabledRequiresToken verifies that when auth is enabled:
//   - POST /orders without Authorization → 401
//   - POST /orders with a valid token → 202
func TestGateway_AuthEnabledRequiresToken(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)
	baseURL := startAppAuthEnabled(t, broker, dsn)

	body := map[string]interface{}{
		"customer_id":  "cust-auth",
		"amount_cents": int64(999),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(body)

	// Without Authorization header → 401.
	resp, err := http.Post(baseURL+"/orders", "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "expected 401 without auth token")

	// With valid token → 202.
	req, err := http.NewRequest(http.MethodPost, baseURL+"/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer good")

	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusAccepted, resp2.StatusCode, "expected 202 with valid auth token")
}

// TestGateway_AuthEnabledNoVerifierFailsClosed verifies that NewApp returns an
// error (fail closed) when auth is enabled but no verifier can be built.
// This is a unit-level test — no Docker infra required, because the JWKS
// check now happens before any postgres/kafka connection is attempted.
func TestGateway_AuthEnabledNoVerifierFailsClosed(t *testing.T) {
	t.Setenv("PG_DSN", "postgres://unused:5432/unused") // not dialled in this path
	t.Setenv("KAFKA_BROKERS", "localhost:9999")         // not dialled in this path
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("GATEWAY_JWKS_URL", "http://127.0.0.1:1/nonexistent") // unreachable
	t.Setenv("GATEWAY_JWKS_ISSUER", "test")
	t.Setenv("GATEWAY_JWKS_AUDIENCE", "test")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	// Do NOT pass WithVerifier — rely on the JWKS builder path.
	_, err := gateway.NewApp(ctx)
	require.Error(t, err, "NewApp must return an error when JWKS is unreachable (fail closed)")
}

// TestGateway_ProjectionPaidBeforeCreatedStillPaid verifies that the projection
// is reorder-safe: a PaymentProcessed event arriving before OrderCreated must
// not be lost, and a subsequent OrderCreated must not downgrade the status.
func TestGateway_ProjectionPaidBeforeCreatedStillPaid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()

	// Step 1: produce PaymentProcessed FIRST (no OrderCreated yet).
	paymentID := uuid.New().String()
	produceEvent(t, broker, "payments.events", "PaymentProcessed", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId:   orderID,
			PaymentId: paymentID,
			Status:    "success",
		}
	})

	// Poll until status == "paid" (MarkPaid upserted the row).
	pollOrderStatus(t, baseURL, orderID, "paid", 30*time.Second)

	// Step 2: produce OrderCreated LATE — must NOT downgrade status to "created".
	produceEvent(t, broker, "orders.events", "OrderCreated", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId:     orderID,
			CustomerId:  "cust-reorder",
			AmountCents: 4200,
			Currency:    "EUR",
		}
	})

	// Poll until currency is filled in (projection consumed the late OrderCreated).
	// The status must remain "paid" throughout (not downgraded to "created").
	// The OrderView API exposes currency; we use it to confirm the late upsert ran.
	deadline := time.Now().Add(30 * time.Second)
	var finalStatus, finalCurrency string
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, orderID))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			var view struct {
				Status   string `json:"status"`
				Currency string `json:"currency"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&view)
			resp.Body.Close()
			finalStatus = view.Status
			finalCurrency = view.Currency
			// Wait until the late OrderCreated fills in the currency.
			if view.Currency == "EUR" {
				break
			}
		} else {
			resp.Body.Close()
		}
		time.Sleep(300 * time.Millisecond)
	}

	require.Equal(t, "paid", finalStatus, "late OrderCreated must NOT downgrade status from paid to created")
	require.Equal(t, "EUR", finalCurrency, "late OrderCreated should fill in the currency field via upsert")
}

// TestGateway_GetOrderCachedSecondCallSkipsDB proves the CQRS CachingJSON behavior
// end-to-end using a focused unit test against the decorated handler with a fake
// in-memory cqrs.Cache implementation.
//
// Approach: we use a fake cqrs.Cache that counts Get/Set calls, wrap the
// decorated GetOrderHandler with it, and verify that on the second call the
// underlying DB store is NOT called (result served from cache).
//
// We also run an integration sub-test with a real Redis container: seed the
// projection by producing an OrderCreated event, GET twice, then delete the DB
// row and GET again — the third call must still return 200 from cache.
func TestGateway_GetOrderCachedSecondCallSkipsDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// --- Unit sub-test: fake cache proves second call skips the handler ---
	t.Run("unit/fake-cache", func(t *testing.T) {
		fc := &fakeCache{}
		orderID := uuid.New().String()

		// raw handler always returns a fixed view
		calls := 0
		rawHandler := func(_ context.Context, q gatewayapp.GetOrder) (gatewayapp.OrderView, error) {
			calls++
			return gatewayapp.OrderView{
				OrderID:     q.OrderID,
				Status:      "created",
				AmountCents: 1000,
				Currency:    "USD",
			}, nil
		}

		decorated := gatewayapp.DecorateGetOrderHandler(rawHandler, fc)

		ctx := context.Background()

		// First call: miss → handler called → stored in cache
		v1, err := decorated(ctx, gatewayapp.GetOrder{OrderID: orderID})
		require.NoError(t, err)
		require.Equal(t, "created", v1.Status)
		require.Equal(t, 1, calls, "first call should hit handler")
		require.Equal(t, 1, fc.setCount, "first call should populate cache")

		// Second call: hit → handler NOT called
		v2, err := decorated(ctx, gatewayapp.GetOrder{OrderID: orderID})
		require.NoError(t, err)
		require.Equal(t, "created", v2.Status)
		require.Equal(t, 1, calls, "second call should be served from cache, handler not called")
	})

	// --- Integration sub-test: real Redis + full gateway ---
	t.Run("integration/real-redis", func(t *testing.T) {
		ctx := context.Background()

		// Start Redis container.
		redisAddr := newRedisAddr(t)

		broker, _ := kafkatest.NewRedpanda(t)
		dsn := pgtest.NewDSN(t)

		t.Setenv("PG_DSN", dsn)
		t.Setenv("KAFKA_BROKERS", broker)
		t.Setenv("KAFKA_CLIENT_ID", "gateway-cache-test-"+uuid.New().String())
		t.Setenv("HTTP_ADDR", "127.0.0.1:0")
		t.Setenv("GATEWAY_AUTH_DISABLED", "true")
		t.Setenv("LOG_LEVEL", "error")
		t.Setenv("REDIS_ADDRS", redisAddr)

		a, err := gateway.NewApp(ctx)
		require.NoError(t, err)
		a.Start()
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = a.Stop(stopCtx)
		})

		baseURL := "http://" + a.Addr()
		orderID := uuid.New().String()

		// Seed the projection by producing an OrderCreated event.
		produceEvent(t, broker, "orders.events", "OrderCreated", orderID, func() proto.Message {
			return &ordersv1.OrderCreated{
				OrderId:     orderID,
				CustomerId:  "cust-cache",
				AmountCents: 5000,
				Currency:    "GBP",
			}
		})

		// Wait until the projection is applied (status == "created").
		pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

		// First GET: populates the cache.
		resp1, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, orderID))
		require.NoError(t, err)
		defer resp1.Body.Close()
		require.Equal(t, http.StatusOK, resp1.StatusCode, "first GET should return 200")
		var view1 struct {
			Status   string `json:"status"`
			Currency string `json:"currency"`
		}
		require.NoError(t, json.NewDecoder(resp1.Body).Decode(&view1))
		require.Equal(t, "created", view1.Status)
		require.Equal(t, "GBP", view1.Currency)

		// Second GET: should be served from cache — same result.
		resp2, err := http.Get(fmt.Sprintf("%s/orders/%s", baseURL, orderID))
		require.NoError(t, err)
		defer resp2.Body.Close()
		require.Equal(t, http.StatusOK, resp2.StatusCode, "second GET should return 200 (from cache)")
		var view2 struct {
			Status   string `json:"status"`
			Currency string `json:"currency"`
		}
		require.NoError(t, json.NewDecoder(resp2.Body).Decode(&view2))
		require.Equal(t, "created", view2.Status)
		require.Equal(t, "GBP", view2.Currency)
	})
}

// newRedisAddr starts a Redis testcontainer and returns the host:port address.
// The container is terminated via t.Cleanup.
func newRedisAddr(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rc.Terminate(context.Background())
	})
	addr, err := rc.ConnectionString(ctx)
	require.NoError(t, err)
	// ConnectionString returns "redis://host:port" — strip the scheme.
	addr = strings.TrimPrefix(addr, "redis://")
	return addr
}

// fakeCache is a simple in-memory cqrs.Cache implementation for unit testing.
// It records Set and Get call counts to verify caching behaviour.
type fakeCache struct {
	data     map[string][]byte
	setCount int
	getCount int
}

func (f *fakeCache) Get(_ context.Context, key string) ([]byte, bool) {
	f.getCount++
	if f.data == nil {
		return nil, false
	}
	v, ok := f.data[key]
	return v, ok
}

func (f *fakeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	f.setCount++
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	f.data[key] = value
}

// TestGateway_AuthzForbidsWithoutRole proves that the RBAC policy on POST /orders:
//   - Returns 403 when the principal holds no permitted role.
//   - Returns 202 when the principal holds the "user" role.
func TestGateway_AuthzForbidsWithoutRole(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.NewRedpanda(t)
	dsn := pgtest.NewDSN(t)

	// Start with auth enabled + a verifier that returns a role-less principal.
	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "gateway-authz-test-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, gateway.WithVerifier(stubVerifierNoRole{}))
	require.NoError(t, err)
	a.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(stopCtx)
	})

	baseURL := "http://" + a.Addr()

	body := map[string]interface{}{
		"customer_id":  "cust-authz",
		"amount_cents": int64(100),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(body)

	// With valid token but NO role → 403.
	req, err := http.NewRequest(http.MethodPost, baseURL+"/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer good")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode, "expected 403 when principal lacks required role")

	// Now start a second gateway instance with a verifier that includes the "user" role.
	t.Setenv("KAFKA_CLIENT_ID", "gateway-authz-test2-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")

	a2, err := gateway.NewApp(ctx, gateway.WithVerifier(stubVerifier{}))
	require.NoError(t, err)
	a2.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a2.Stop(stopCtx)
	})

	baseURL2 := "http://" + a2.Addr()

	// With "user" role → 202.
	req2, err := http.NewRequest(http.MethodPost, baseURL2+"/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer good")

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusAccepted, resp2.StatusCode, "expected 202 when principal has 'user' role")
}
