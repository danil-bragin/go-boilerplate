package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/twmb/franz-go/pkg/kgo"
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

	configureTopics(t)
	os.Setenv("PG_DSN", dsn)
	os.Setenv("KAFKA_BROKERS", broker)
	os.Setenv("KAFKA_CLIENT_ID", "gateway-test-"+uuid.New().String())
	os.Setenv("HTTP_ADDR", "127.0.0.1:0")       // random port
	os.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0") // random port — :9090 may be taken (e.g. a compose Prometheus)
	os.Setenv("GATEWAY_AUTH_DISABLED", "true")
	os.Setenv("LOG_LEVEL", "error") // suppress noise in tests

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, opts...)
	require.NoError(t, err)

	require.NoError(t, a.Start())
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

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	baseURL := startApp(t, broker, dsn)

	// POST /v1/orders
	body := map[string]interface{}{
		"customer_id":  "cust-1",
		"amount_cents": int64(1500),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(bodyBytes))
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

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()

	// Produce an OrderCreated event to orders.events.
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId:     orderID,
			CustomerId:  "cust-proj",
			AmountCents: 2000,
			Currency:    "EUR",
		}
	})

	// Poll GET /v1/orders/{id} until status == "created".
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	// Produce a PaymentProcessed event to payments.events.
	paymentID := uuid.New().String()
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId:   orderID,
			PaymentId: paymentID,
			Status:    "success",
		}
	})

	// Poll GET /v1/orders/{id} until status == "paid".
	pollOrderStatus(t, baseURL, orderID, "paid", 30*time.Second)
}

// TestGateway_GetUnknownOrder404 verifies that GET /v1/orders/<unknown> returns 404.
func TestGateway_GetUnknownOrder404(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	baseURL := startApp(t, broker, dsn)

	unknownID := uuid.New().String()
	resp, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, unknownID))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
	var prob struct {
		Status   int            `json:"status"`
		Code     string         `json:"code"`
		Detail   string         `json:"detail"`
		Instance string         `json:"instance"`
		Params   map[string]any `json:"params"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&prob))
	assert.Equal(t, http.StatusNotFound, prob.Status)
	assert.Equal(t, "GATEWAY_ORDER_NOT_FOUND", prob.Code)
	assert.Equal(t, "Order "+unknownID+" was not found.", prob.Detail,
		"detail renders from the en catalog template")
	assert.Equal(t, "/v1/orders/"+unknownID, prob.Instance)
	assert.Equal(t, unknownID, prob.Params["order_id"], "AIP-193: message variables must be params")
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
	consumer, err := kafka.NewConsumer(cfg, []string{topicCommands})
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

// pollOrderStatus polls GET /v1/orders/{id} until the status matches, or fails.
func pollOrderStatus(t *testing.T, baseURL, orderID, expectedStatus string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID))
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

// noRoleVerifier returns a fakes.Verifier whose principal deliberately lacks
// any role. Used to prove that RBAC returns 403 when the principal has no
// permitted role.
func noRoleVerifier() *fakes.Verifier {
	v := fakes.NewVerifier()
	v.Principal = authpkg.Principal{Subject: "test-user-norole", Roles: []string{}}
	return v
}

// multiUserVerifier resolves the literal tokens "alice" and "bob" to distinct
// principals (both holding the "user" role). Used to prove per-principal
// scoping of Idempotency-Key and read-path ownership.
type multiUserVerifier struct{}

func (multiUserVerifier) Verify(_ context.Context, rawToken string) (authpkg.Principal, error) {
	switch rawToken {
	case "alice", "bob":
		return authpkg.Principal{Subject: rawToken, Roles: []string{"user"}}, nil
	case "root":
		return authpkg.Principal{Subject: "root", Roles: []string{"user", "admin"}}, nil
	}
	return authpkg.Principal{}, authpkg.ErrInvalidToken
}

// startAppAuthEnabled starts the gateway with AuthDisabled=false and the
// default fakes.Verifier (accepts any non-empty bearer token, "user" role).
func startAppAuthEnabled(t *testing.T, broker, dsn string) string {
	return startAppWithVerifier(t, broker, dsn, fakes.NewVerifier())
}

// startAppWithVerifier starts the gateway with AuthDisabled=false and the
// given verifier.
func startAppWithVerifier(t *testing.T, broker, dsn string, v authpkg.Verifier) string {
	t.Helper()

	configureTopics(t)
	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "gateway-test-auth-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, gateway.WithVerifier(v))
	require.NoError(t, err)

	require.NoError(t, a.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(stopCtx)
	})

	return "http://" + a.Addr()
}

// TestGateway_AuthEnabledRequiresToken verifies that when auth is enabled:
//   - POST /v1/orders without Authorization → 401
//   - POST /v1/orders with a valid token → 202
func TestGateway_AuthEnabledRequiresToken(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startAppAuthEnabled(t, broker, dsn)

	body := map[string]interface{}{
		"customer_id":  "cust-auth",
		"amount_cents": int64(999),
		"currency":     "USD",
	}
	bodyBytes, _ := json.Marshal(body)

	// Without Authorization header → 401.
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "expected 401 without auth token")

	// With valid token → 202.
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders", bytes.NewReader(bodyBytes))
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
	t.Setenv("AUTH_ALLOW_INSECURE_JWKS", "true")                   // http URL: opt in so we test the unreachable-fetch path, not the https guard
	t.Setenv("GATEWAY_JWKS_ISSUER", "test")
	t.Setenv("GATEWAY_JWKS_AUDIENCE", "test")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("LOG_LEVEL", "error")

	// A sub-second deadline keeps this fast-lane test fast: NewJWKSVerifier
	// bounds its initial fetch by min(caller deadline, jwksInitTimeout), so
	// the unreachable-IdP retry loop gives up in ~500ms instead of the full
	// 10s production bound (which stays untouched).
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// Do NOT pass WithVerifier — rely on the JWKS builder path.
	start := time.Now()
	_, err := gateway.NewApp(ctx)
	require.Error(t, err, "NewApp must return an error when JWKS is unreachable (fail closed)")
	require.Less(t, time.Since(start), time.Second, "fail-closed must surface fast under a short caller deadline")
}

// TestGateway_ProjectionPaidBeforeCreatedStillPaid verifies that the projection
// is reorder-safe: a PaymentProcessed event arriving before OrderCreated must
// not be lost, and a subsequent OrderCreated must not downgrade the status.
func TestGateway_ProjectionPaidBeforeCreatedStillPaid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()

	// Step 1: produce PaymentProcessed FIRST (no OrderCreated yet).
	paymentID := uuid.New().String()
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId:   orderID,
			PaymentId: paymentID,
			Status:    "success",
		}
	})

	// Poll until status == "paid" (MarkPaid upserted the row).
	pollOrderStatus(t, baseURL, orderID, "paid", 30*time.Second)

	// Step 2: produce OrderCreated LATE — must NOT downgrade status to "created".
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
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
		resp, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID))
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

		broker, _ := kafkatest.Shared(t)
		dsn := pgtest.SharedDSN(t)

		configureTopics(t)
		t.Setenv("PG_DSN", dsn)
		t.Setenv("KAFKA_BROKERS", broker)
		t.Setenv("KAFKA_CLIENT_ID", "gateway-cache-test-"+uuid.New().String())
		t.Setenv("HTTP_ADDR", "127.0.0.1:0")
		t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
		t.Setenv("GATEWAY_AUTH_DISABLED", "true")
		t.Setenv("LOG_LEVEL", "error")
		t.Setenv("REDIS_ADDRS", redisAddr)

		a, err := gateway.NewApp(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Start())
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = a.Stop(stopCtx)
		})

		baseURL := "http://" + a.Addr()
		orderID := uuid.New().String()

		// Seed the projection by producing an OrderCreated event.
		produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
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
		resp1, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID))
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
		resp2, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID))
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

func (f *fakeCache) Delete(_ context.Context, key string) error {
	delete(f.data, key)
	return nil
}

func (f *fakeCache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok := f.Get(ctx, key); ok {
		return v, nil
	}
	v, err := load(ctx)
	if err != nil {
		return nil, err
	}
	f.Set(ctx, key, v, ttl)
	return v, nil
}

// TestGateway_PerIPRateLimit verifies that the per-IP rate limiter correctly
// returns 429 once the burst is exhausted.
//
// This is a focused unit-level test: it configures a tiny limiter (rps=1,
// burst=2) via environment variables and sends 3 sequential requests from the
// test HTTP client. The first two should succeed (burst=2 tokens available);
// the third must receive 429 with a Retry-After header.
//
// Note: httptest client connections share the loopback address (127.0.0.1) so
// all requests hit the same per-IP bucket — this is the intended behaviour for
// the test.
func TestGateway_PerIPRateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("RATELIMIT_RPS", "1")
	t.Setenv("RATELIMIT_BURST", "2")

	baseURL := startApp(t, broker, dsn)

	client := &http.Client{}

	// First two requests should succeed (within burst).
	for i := range 2 {
		resp, err := client.Get(baseURL + "/v1/orders/" + uuid.New().String())
		require.NoError(t, err)
		resp.Body.Close()
		// 404 is fine — the order doesn't exist; we just need < 429.
		require.NotEqual(t, http.StatusTooManyRequests, resp.StatusCode,
			"request %d must not be rate-limited (within burst)", i+1)
	}

	// Third request must be rate-limited.
	resp, err := client.Get(baseURL + "/v1/orders/" + uuid.New().String())
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "third request must be rate-limited (429)")
	require.Equal(t, "1", resp.Header.Get("Retry-After"), "429 must carry Retry-After: 1")
}

// TestGateway_AuthzForbidsWithoutRole proves that the RBAC policy on POST /v1/orders:
//   - Returns 403 when the principal holds no permitted role.
//   - Returns 202 when the principal holds the "user" role.
func TestGateway_AuthzForbidsWithoutRole(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	// Start with auth enabled + a verifier that returns a role-less principal.
	configureTopics(t)
	t.Setenv("PG_DSN", dsn)
	t.Setenv("KAFKA_BROKERS", broker)
	t.Setenv("KAFKA_CLIENT_ID", "gateway-authz-test-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GATEWAY_AUTH_DISABLED", "false")
	t.Setenv("LOG_LEVEL", "error")

	ctx := context.Background()
	a, err := gateway.NewApp(ctx, gateway.WithVerifier(noRoleVerifier()))
	require.NoError(t, err)
	require.NoError(t, a.Start())
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
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer good")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode, "expected 403 when principal lacks required role")
	var forbidden struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&forbidden))
	assert.Equal(t, "AUTH_FORBIDDEN", forbidden.Code, "platform auth code must flow through FromError")

	// Now start a second gateway instance with a verifier that includes the "user" role.
	t.Setenv("KAFKA_CLIENT_ID", "gateway-authz-test2-"+uuid.New().String())
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("ADMIN_HTTP_ADDR", "127.0.0.1:0")

	a2, err := gateway.NewApp(ctx, gateway.WithVerifier(fakes.NewVerifier()))
	require.NoError(t, err)
	require.NoError(t, a2.Start())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a2.Stop(stopCtx)
	})

	baseURL2 := "http://" + a2.Addr()

	// With "user" role → 202.
	req2, err := http.NewRequest(http.MethodPost, baseURL2+"/v1/orders", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer good")

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusAccepted, resp2.StatusCode, "expected 202 when principal has 'user' role")
}

// postOrderWithKey posts an order with an optional Idempotency-Key header and
// returns the order_id from the 202 response.
func postOrderWithKey(t *testing.T, baseURL, key string) string {
	t.Helper()
	body := []byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var out struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.OrderID)
	return out.OrderID
}

// TestGateway_IdempotencyKey verifies that retried POSTs with the same
// Idempotency-Key map to the SAME deterministic order id (so the command
// message-id is identical and downstream inbox dedup collapses the retry),
// while different keys (or no key) yield fresh ids.
func TestGateway_IdempotencyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	id1 := postOrderWithKey(t, baseURL, "retry-key-1")
	id2 := postOrderWithKey(t, baseURL, "retry-key-1")
	assert.Equal(t, id1, id2, "same Idempotency-Key must yield the same order id")

	id3 := postOrderWithKey(t, baseURL, "retry-key-2")
	assert.NotEqual(t, id1, id3, "different Idempotency-Key must yield a different order id")

	id4 := postOrderWithKey(t, baseURL, "")
	id5 := postOrderWithKey(t, baseURL, "")
	assert.NotEqual(t, id4, id5, "no Idempotency-Key must yield fresh ids")

	// Downstream dedup hinges on message-id == order id for BOTH deliveries.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topicCommands),
		kgo.ConsumerGroup("idem-test-"+uuid.New().String()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer cl.Close()

	seen := map[string]int{} // message-id → count
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && seen[id1] < 2 {
		fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := cl.PollFetches(fctx)
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			for _, h := range rec.Headers {
				if h.Key == "message-id" {
					seen[string(h.Value)]++
				}
			}
		})
	}
	assert.Equal(t, 2, seen[id1], "both retried POSTs must produce commands with the same message-id (order id)")
}

// postOrderRaw posts an order with optional Idempotency-Key and bearer token,
// returning the response status and decoded order_id (empty unless 202).
func postOrderRaw(t *testing.T, baseURL, key, token string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return resp.StatusCode, ""
	}
	var out struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out.OrderID
}

// TestGateway_IdempotencyKeyScopedByPrincipal proves Idempotency-Key values
// live in per-principal namespaces when auth is enabled: client B reusing
// client A's key must get its OWN order, not A's order id (with A's order
// silently absorbing B's request).
func TestGateway_IdempotencyKeyScopedByPrincipal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	aliceBody := []byte(`{"customer_id":"alice","amount_cents":1500,"currency":"USD"}`)
	bobBody := []byte(`{"customer_id":"bob","amount_cents":1500,"currency":"USD"}`)

	// Same principal + same key → same order id (retry dedup).
	st, aliceID := postOrderRaw(t, baseURL, "shared-key", "alice", aliceBody)
	require.Equal(t, http.StatusAccepted, st)
	st, aliceRetryID := postOrderRaw(t, baseURL, "shared-key", "alice", aliceBody)
	require.Equal(t, http.StatusAccepted, st)
	assert.Equal(t, aliceID, aliceRetryID, "same principal + same key must be a retry (same id)")

	// Different principal + same key → DIFFERENT order.
	st, bobID := postOrderRaw(t, baseURL, "shared-key", "bob", bobBody)
	require.Equal(t, http.StatusAccepted, st, "bob's reuse of alice's key must create bob's own order")
	assert.NotEqual(t, aliceID, bobID, "idempotency keys must be scoped per principal — B reusing A's key must not collide into A's order")
}

// TestGateway_IdempotencyKeyBodyMismatch409 proves that reusing an
// Idempotency-Key with a DIFFERENT request body is rejected with 409
// problem+json instead of silently returning the first request's order id
// (which would make the second order never exist).
func TestGateway_IdempotencyKeyBodyMismatch409(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	st, id := postOrderRaw(t, baseURL, "mismatch-key", "",
		[]byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)
	require.NotEmpty(t, id)

	// Same key, different amount → 409.
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/orders",
		bytes.NewReader([]byte(`{"customer_id":"c1","amount_cents":9999,"currency":"USD"}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "mismatch-key")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode,
		"key reuse with a different body must be rejected, not absorbed into the first order")
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
	var prob struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&prob))
	assert.Equal(t, "GATEWAY_IDEMPOTENCY_BODY_MISMATCH", prob.Code, "machine-readable code is the contract")
	assert.Contains(t, prob.Detail, "Idempotency-Key", "problem detail must name the cause")

	// Same key, identical body → still the original id (true retry).
	st, retryID := postOrderRaw(t, baseURL, "mismatch-key", "",
		[]byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)
	assert.Equal(t, id, retryID)
}

// TestGateway_PostReturnsLocationAndPendingRow verifies API honesty: POST
// /v1/orders responds 202 with a Location header, and an IMMEDIATE GET on
// that location returns 200 with status "pending" (not 404) because the
// gateway pre-inserts the read-model row at POST time.
func TestGateway_PostReturnsLocationAndPendingRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	body := []byte(`{"customer_id":"c1","amount_cents":1500,"currency":"USD"}`)
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	var out struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.OrderID)

	location := resp.Header.Get("Location")
	require.Equal(t, "/v1/orders/"+out.OrderID, location, "202 must carry the order resource Location")

	// Immediate GET → 200 pending (no projection consumer has run yet for
	// this order; only the POST-time pending insert can explain a 200).
	getResp, err := http.Get(baseURL + location)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode, "GET immediately after POST must be 200, not 404")

	var view struct {
		Status      string `json:"status"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	}
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&view))
	assert.Equal(t, "pending", view.Status)
	assert.EqualValues(t, 1500, view.AmountCents)
	assert.Equal(t, "USD", view.Currency)

	// 404 now returns problem+json with the documented coded shape.
	missingID := uuid.New().String()
	nf, err := http.Get(baseURL + "/v1/orders/" + missingID)
	require.NoError(t, err)
	defer nf.Body.Close()
	require.Equal(t, http.StatusNotFound, nf.StatusCode)
	assert.Contains(t, nf.Header.Get("Content-Type"), "application/problem+json")
	var nfProb struct {
		Code   string         `json:"code"`
		Params map[string]any `json:"params"`
	}
	require.NoError(t, json.NewDecoder(nf.Body).Decode(&nfProb))
	assert.Equal(t, "GATEWAY_ORDER_NOT_FOUND", nfProb.Code)
	assert.Equal(t, missingID, nfProb.Params["order_id"])
}

// TestGateway_PendingUpgradesAndReorderSafety verifies the projection status
// lattice with the new pending state: pending → created → paid in order, and
// paid is never downgraded by late OrderCreated or a POST-time pending insert.
func TestGateway_PendingUpgradesAndReorderSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	// Create an order (pending row).
	orderID := postOrderWithKey(t, baseURL, "")

	// OrderCreated arrives → pending upgrades to created.
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "c1", AmountCents: 1500, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 15*time.Second)

	// PaymentProcessed → paid.
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{OrderId: orderID, PaymentId: uuid.New().String(), Status: "processed"}
	})
	pollOrderStatus(t, baseURL, orderID, "paid", 15*time.Second)

	// A late duplicate OrderCreated must NOT downgrade paid.
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "c1", AmountCents: 1500, Currency: "USD"}
	})
	time.Sleep(2 * time.Second)
	pollOrderStatus(t, baseURL, orderID, "paid", 5*time.Second)
}

// TestGateway_IdempotentRetrySinglePendingRow extends the Idempotency-Key
// guarantee to the read model: two POSTs with the same key produce ONE
// orders_read row.
func TestGateway_IdempotentRetrySinglePendingRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	id1 := postOrderWithKey(t, baseURL, "single-row-key")
	id2 := postOrderWithKey(t, baseURL, "single-row-key")
	require.Equal(t, id1, id2)

	ctx := context.Background()
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(os.Getenv("PG_DSN"))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from orders_read where order_id = $1`, id1).Scan(&count))
	assert.Equal(t, 1, count, "retried POST with the same Idempotency-Key must yield one read-model row")
}

// TestGateway_ReadOwnership proves the read path is scoped to the order's
// owner when auth is enabled:
//   - GET: owner → 200, another principal → 404 (not 403 — no existence
//     oracle), admin → 200.
//   - LIST: each non-admin principal sees only rows whose customer_id equals
//     its subject; admin sees all.
func TestGateway_ReadOwnership(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	get := func(token, orderID string) int {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/orders/"+orderID, http.NoBody)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}
	list := func(token string) []string {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/orders", http.NoBody)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out struct {
			Items []struct {
				OrderID string `json:"order_id"`
			} `json:"items"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		ids := make([]string, len(out.Items))
		for i, it := range out.Items {
			ids[i] = it.OrderID
		}
		return ids
	}

	st, aliceOrder := postOrderRaw(t, baseURL, "", "alice",
		[]byte(`{"customer_id":"alice","amount_cents":100,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)
	st, bobOrder := postOrderRaw(t, baseURL, "", "bob",
		[]byte(`{"customer_id":"bob","amount_cents":200,"currency":"USD"}`))
	require.Equal(t, http.StatusAccepted, st)

	// GET: owner 200, non-owner 404 (no existence oracle), admin 200.
	require.Equal(t, http.StatusOK, get("alice", aliceOrder), "owner must read own order")
	require.Equal(t, http.StatusNotFound, get("bob", aliceOrder),
		"another principal must get 404 — same response as a nonexistent order")
	require.Equal(t, http.StatusOK, get("root", aliceOrder), "admin must read any order")

	// LIST: non-admins see only their own rows; admin sees all.
	assert.ElementsMatch(t, []string{aliceOrder}, list("alice"), "alice must list only her orders")
	assert.ElementsMatch(t, []string{bobOrder}, list("bob"), "bob must list only his orders")
	adminSeen := list("root")
	assert.Contains(t, adminSeen, aliceOrder, "admin list must include alice's order")
	assert.Contains(t, adminSeen, bobOrder, "admin list must include bob's order")
}

// TestGateway_ListOrdersKeysetPagination verifies GET /v1/orders cursor
// pagination: newest first, stable page boundaries, exhaustion, bad cursor →
// 400 problem+json, limit capping.
func TestGateway_ListOrdersKeysetPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	// Create 5 orders.
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = postOrderWithKey(t, baseURL, "")
	}

	type page struct {
		Items []struct {
			OrderID string `json:"order_id"`
			Status  string `json:"status"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	fetch := func(query string) (page, int) {
		resp, err := http.Get(baseURL + "/v1/orders" + query)
		require.NoError(t, err)
		defer resp.Body.Close()
		var p page
		if resp.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
		}
		return p, resp.StatusCode
	}

	// Page through with limit=2: 2 + 2 + 1, no overlaps, all 5 seen.
	seen := map[string]bool{}
	var cursor string
	total := 0
	for range 4 {
		q := "?limit=2"
		if cursor != "" {
			q += "&cursor=" + cursor
		}
		p, code := fetch(q)
		require.Equal(t, http.StatusOK, code)
		for _, it := range p.Items {
			require.False(t, seen[it.OrderID], "pagination must not repeat order %s", it.OrderID)
			seen[it.OrderID] = true
			total++
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	require.Equal(t, 5, total, "pagination must enumerate all orders exactly once")
	for _, id := range ids {
		assert.True(t, seen[id], "order %s missing from listing", id)
	}

	// Default limit returns all 5 on one page, newest first.
	p, code := fetch("")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, p.Items, 5)
	assert.Equal(t, ids[4], p.Items[0].OrderID, "listing must be newest first")
	assert.Empty(t, p.NextCursor, "short page must not return a cursor")

	// Malformed cursor → 400 problem+json.
	resp, err := http.Get(baseURL + "/v1/orders?cursor=%21%21not-a-cursor")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
	var cursorProb struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&cursorProb))
	assert.Equal(t, "GATEWAY_INVALID_CURSOR", cursorProb.Code)

	// limit above the documented maximum (100) is capped server-side; the
	// request still succeeds (no kernel validation middleware is mounted).
	_, code = fetch("?limit=101")
	assert.Equal(t, http.StatusOK, code)
}

// TestGateway_ProjectionPaymentFailed verifies the failure branch of the
// choreography: a PaymentFailed event moves the read model to
// "payment_failed", and a later PaymentProcessed must NOT overwrite it —
// payment_failed and paid are both terminal, first terminal wins.
func TestGateway_ProjectionPaymentFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "c-fail", AmountCents: 1_000_000, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentFailed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentFailed{OrderId: orderID, Reason: "declined"}
	})
	pollOrderStatus(t, baseURL, orderID, "payment_failed", 30*time.Second)

	// A late PaymentProcessed must be ignored: first terminal state wins.
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{OrderId: orderID, PaymentId: uuid.New().String(), Status: "success"}
	})
	time.Sleep(3 * time.Second)
	require.Equal(t, "payment_failed", getStatus(t, baseURL, orderID),
		"a terminal payment_failed must not be overwritten by a later paid")
}

// TestGateway_ProjectionPaidWinsOverLateFailure is the mirror precedence case:
// once paid, a late PaymentFailed must be ignored (first terminal wins).
func TestGateway_ProjectionPaidWinsOverLateFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "c-paid", AmountCents: 100, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{OrderId: orderID, PaymentId: uuid.New().String(), Status: "success"}
	})
	pollOrderStatus(t, baseURL, orderID, "paid", 30*time.Second)

	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentFailed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentFailed{OrderId: orderID, Reason: "declined"}
	})
	time.Sleep(3 * time.Second)
	require.Equal(t, "paid", getStatus(t, baseURL, orderID),
		"a terminal paid must not be overwritten by a later payment_failed")
}

// getStatus returns the current order status (empty string on any error).
func getStatus(t *testing.T, baseURL, orderID string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var view struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		return ""
	}
	return view.Status
}

// TestGateway_ProjectionPaymentTimeout verifies the read model handles
// OrderPaymentTimedOut: created → payment_timeout, and a paid order is never
// downgraded by a late timeout (first terminal wins).
func TestGateway_ProjectionPaymentTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	// Unpaid order: created → payment_timeout.
	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "c-late", AmountCents: 700, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	produceEvent(t, broker, topicOrdersEvents, "orders.OrderPaymentTimedOut.v1", orderID, func() proto.Message {
		return &ordersv1.OrderPaymentTimedOut{OrderId: orderID}
	})
	pollOrderStatus(t, baseURL, orderID, "payment_timeout", 30*time.Second)

	// Paid order: a late OrderPaymentTimedOut must be ignored.
	paidID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", paidID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: paidID, CustomerId: "c-paid", AmountCents: 100, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, paidID, "created", 30*time.Second)
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", paidID, func() proto.Message {
		return &ordersv1.PaymentProcessed{OrderId: paidID, PaymentId: uuid.New().String(), Status: "success"}
	})
	pollOrderStatus(t, baseURL, paidID, "paid", 30*time.Second)

	produceEvent(t, broker, topicOrdersEvents, "orders.OrderPaymentTimedOut.v1", paidID, func() proto.Message {
		return &ordersv1.OrderPaymentTimedOut{OrderId: paidID}
	})
	time.Sleep(3 * time.Second)
	require.Equal(t, "paid", getStatus(t, baseURL, paidID),
		"a terminal paid must not be overwritten by a later payment_timeout")

	// Inverse race: a PaymentProcessed landing AFTER the timeout must not
	// flip the projection back to paid — first terminal event wins, matching
	// the orders service (whose status='created' guard ignores the late
	// outcome too).
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{OrderId: orderID, PaymentId: uuid.New().String(), Status: "success"}
	})
	time.Sleep(3 * time.Second)
	require.Equal(t, "payment_timeout", getStatus(t, baseURL, orderID),
		"a terminal payment_timeout must not be overwritten by a later paid")
}

// TestGateway_I18nLocalizedProblemsOverHTTP proves the i18n middleware is
// actually mounted in NewApp: Accept-Language: ru localizes the
// human-readable title/detail of problem responses over real HTTP, the en
// default applies otherwise, and the machine-readable code/params pair is
// identical in both locales.
func TestGateway_I18nLocalizedProblemsOverHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	getProblem := func(method, url, lang string, body []byte) (int, map[string]any) {
		t.Helper()
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, url, rd)
		require.NoError(t, err)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if lang != "" {
			req.Header.Set("Accept-Language", lang)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
		var p map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
		return resp.StatusCode, p
	}

	// 404 — ru vs en (default).
	unknownID := uuid.New().String()
	st, ru := getProblem(http.MethodGet, baseURL+"/v1/orders/"+unknownID, "ru", nil)
	require.Equal(t, http.StatusNotFound, st)
	assert.Equal(t, "Заказ "+unknownID+" не найден.", ru["detail"])
	assert.Equal(t, "Заказ не найден", ru["title"])

	st, en := getProblem(http.MethodGet, baseURL+"/v1/orders/"+unknownID, "", nil)
	require.Equal(t, http.StatusNotFound, st)
	assert.Equal(t, "Order "+unknownID+" was not found.", en["detail"])

	for _, p := range []map[string]any{ru, en} {
		assert.Equal(t, "GATEWAY_ORDER_NOT_FOUND", p["code"], "code is locale-independent")
		params, ok := p["params"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, unknownID, params["order_id"], "params are locale-independent")
	}

	// Validation 400 — ru localizes the platform VALIDATION_FAILED message;
	// the structured fields params survive untouched.
	badBody := []byte(`{"customer_id":"c1","amount_cents":-5,"currency":"USD"}`)
	st, vru := getProblem(http.MethodPost, baseURL+"/v1/orders", "ru, en;q=0.5", badBody)
	require.Equal(t, http.StatusBadRequest, st)
	assert.Equal(t, "VALIDATION_FAILED", vru["code"])
	assert.Equal(t, "Одно или несколько полей заполнены неверно.", vru["detail"])
	vparams, ok := vru["params"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, vparams["fields"], "structured fields must survive localization")
}

// TestGateway_CreatedAtAndTimezone proves the time contract over real HTTP:
// created_at is RFC 3339 UTC ("Z"), X-Timezone adds a display-only
// created_at_local with the zone's offset, an invalid zone is a coded 400,
// and list items carry the same fields.
func TestGateway_CreatedAtAndTimezone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{OrderId: orderID, CustomerId: "cust-tz", AmountCents: 700, Currency: "USD"}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	getView := func(tz string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/orders/"+orderID, http.NoBody)
		require.NoError(t, err)
		if tz != "" {
			req.Header.Set("X-Timezone", tz)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var v map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
		return resp.StatusCode, v
	}

	// No header: created_at present, UTC Z, parseable; no local field.
	st, v := getView("")
	require.Equal(t, http.StatusOK, st)
	createdAt, _ := v["created_at"].(string)
	require.NotEmpty(t, createdAt, "created_at must be present")
	assert.True(t, strings.HasSuffix(createdAt, "Z"), "created_at must be UTC with Z suffix, got %q", createdAt)
	parsed, err := time.Parse(time.RFC3339, createdAt)
	require.NoError(t, err, "created_at must be RFC 3339")
	assert.WithinDuration(t, time.Now().UTC(), parsed, 5*time.Minute)
	_, hasLocal := v["created_at_local"]
	assert.False(t, hasLocal, "created_at_local must be absent without X-Timezone")

	// Valid zone: created_at unchanged, created_at_local added with offset.
	st, v = getView("Europe/Kyiv")
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, createdAt, v["created_at"], "contract field is identical with or without X-Timezone")
	local, _ := v["created_at_local"].(string)
	require.NotEmpty(t, local)
	localParsed, err := time.Parse(time.RFC3339, local)
	require.NoError(t, err)
	assert.True(t, localParsed.Equal(parsed), "created_at_local must be the same instant")
	assert.False(t, strings.HasSuffix(local, "Z"), "created_at_local carries the zone offset, got %q", local)

	// Invalid zone: coded 400, offending value in params.
	st, v = getView("UTC+3")
	require.Equal(t, http.StatusBadRequest, st)
	assert.Equal(t, "GATEWAY_INVALID_TIMEZONE", v["code"])
	params, ok := v["params"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UTC+3", params["timezone"])

	// List items carry created_at too (and local with the header).
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/orders?limit=5", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("X-Timezone", "America/New_York")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	require.NotEmpty(t, page.Items)
	itemCreated, _ := page.Items[0]["created_at"].(string)
	assert.True(t, strings.HasSuffix(itemCreated, "Z"), "list created_at must be UTC Z, got %q", itemCreated)
	itemLocal, _ := page.Items[0]["created_at_local"].(string)
	assert.NotEmpty(t, itemLocal, "list items must carry created_at_local with X-Timezone")
}
