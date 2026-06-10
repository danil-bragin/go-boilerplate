package gateway_test

// Integration tests for GET /v1/orders/{id}/events (Server-Sent Events):
// live status streaming via Redis pub/sub, polling fallback without Redis,
// GET-equivalent ownership semantics, and Last-Event-ID resume.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// sseEvent is one parsed SSE event (heartbeat comments are skipped).
type sseEvent struct {
	ID     string
	Status string // parsed from the JSON data payload
}

// openSSE connects to GET /v1/orders/{id}/events and returns a channel of
// parsed events plus the response status code. headers are added verbatim
// (Authorization, Last-Event-ID). The stream is closed via t.Cleanup.
func openSSE(t *testing.T, baseURL, orderID string, headers map[string]string) (<-chan sseEvent, *http.Response) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/orders/%s/events", baseURL, orderID), http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{} // no timeout: the stream is long-lived
	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	events := make(chan sseEvent, 16)
	if resp.StatusCode != http.StatusOK {
		close(events)
		return events, resp
	}
	require.Equal(t, "text/event-stream", strings.Split(resp.Header.Get("Content-Type"), ";")[0])

	go func() {
		defer close(events)
		scanner := bufio.NewScanner(resp.Body)
		var id, data string
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, ":"): // heartbeat comment
			case strings.HasPrefix(line, "id:"):
				id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			case line == "" && data != "":
				var payload struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal([]byte(data), &payload)
				events <- sseEvent{ID: id, Status: payload.Status}
				id, data = "", ""
			}
		}
	}()
	return events, resp
}

// nextEvent waits for the next SSE event or fails the test.
func nextEvent(t *testing.T, events <-chan sseEvent, within time.Duration, hint string) sseEvent {
	t.Helper()
	select {
	case evt, ok := <-events:
		require.True(t, ok, "SSE stream closed while waiting for %s", hint)
		return evt
	case <-time.After(within):
		t.Fatalf("no SSE event within %v while waiting for %s", within, hint)
		return sseEvent{}
	}
}

// postOrder POSTs a new order and returns its id.
func postOrder(t *testing.T, baseURL string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"customer_id": "cust-sse", "amount_cents": int64(1200), "currency": "USD",
	})
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(body))
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

// TestGateway_SSE_StreamsStatusSequence_Redis: with Redis configured the
// stream delivers the initial row status immediately and then live
// pending→created→paid transitions pushed by the projection via pub/sub.
func TestGateway_SSE_StreamsStatusSequence_Redis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	redisAddr := newRedisAddr(t)
	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("REDIS_ADDRS", redisAddr)
	baseURL := startApp(t, broker, dsn)

	orderID := postOrder(t, baseURL)

	events, _ := openSSE(t, baseURL, orderID, nil) //nolint:bodyclose // closed via t.Cleanup in openSSE

	// Initial event: the pre-inserted read-model row (status=pending).
	first := nextEvent(t, events, 10*time.Second, "initial event")
	require.Equal(t, "pending", first.Status)
	require.Equal(t, "0", first.ID)

	// Projection applies OrderCreated → live event via Redis pub/sub.
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId: orderID, CustomerId: "cust-sse", AmountCents: 1200, Currency: "USD",
		}
	})
	created := nextEvent(t, events, 30*time.Second, "created event")
	require.Equal(t, "created", created.Status)
	require.Equal(t, "1", created.ID)

	// Projection applies PaymentProcessed → paid.
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId: orderID, PaymentId: uuid.New().String(), Status: "success",
		}
	})
	paid := nextEvent(t, events, 30*time.Second, "paid event")
	require.Equal(t, "paid", paid.Status)
	require.Equal(t, "2", paid.ID)
}

// TestGateway_SSE_PollingFallbackWithoutRedis: with no Redis configured the
// stream still works by polling the projection store.
func TestGateway_SSE_PollingFallbackWithoutRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("REDIS_ADDRS", "")                    // explicit: no Redis → polling path
	t.Setenv("GATEWAY_SSE_POLL_INTERVAL", "200ms") // fast polls for test speed
	baseURL := startApp(t, broker, dsn)

	orderID := postOrder(t, baseURL)

	events, _ := openSSE(t, baseURL, orderID, nil) //nolint:bodyclose // closed via t.Cleanup in openSSE
	require.Equal(t, "pending", nextEvent(t, events, 10*time.Second, "initial event").Status)

	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId: orderID, CustomerId: "cust-sse", AmountCents: 1200, Currency: "USD",
		}
	})
	require.Equal(t, "created", nextEvent(t, events, 30*time.Second, "created event (polled)").Status)

	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId: orderID, PaymentId: uuid.New().String(), Status: "success",
		}
	})
	require.Equal(t, "paid", nextEvent(t, events, 30*time.Second, "paid event (polled)").Status)
}

// TestGateway_SSE_OwnershipSemantics: the stream applies the SAME ownership
// rules as GET /v1/orders/{id} — non-owners get the same 404 as a
// nonexistent order (no existence oracle), admins bypass, missing token 401.
func TestGateway_SSE_OwnershipSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("REDIS_ADDRS", "")
	t.Setenv("GATEWAY_SSE_POLL_INTERVAL", "200ms")
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	// Seed an order owned by alice.
	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId: orderID, CustomerId: "alice", AmountCents: 900, Currency: "USD",
		}
	})
	pollOrderStatusAs(t, baseURL, orderID, "created", "alice", 30*time.Second)

	// No token → 401.
	_, resp := openSSE(t, baseURL, orderID, nil) //nolint:bodyclose // closed via t.Cleanup in openSSE
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Non-owner → 404 (same shape as a nonexistent order).
	_, resp = openSSE(t, baseURL, orderID, map[string]string{"Authorization": "Bearer bob"}) //nolint:bodyclose // closed via t.Cleanup in openSSE
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Nonexistent order for an authenticated user → 404.
	_, resp = openSSE(t, baseURL, uuid.New().String(), map[string]string{"Authorization": "Bearer alice"}) //nolint:bodyclose // closed via t.Cleanup in openSSE
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Owner → 200 + initial event.
	events, resp := openSSE(t, baseURL, orderID, map[string]string{"Authorization": "Bearer alice"}) //nolint:bodyclose // closed via t.Cleanup in openSSE
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "created", nextEvent(t, events, 10*time.Second, "owner initial event").Status)

	// Admin (not the owner) → 200.
	adminEvents, resp := openSSE(t, baseURL, orderID, map[string]string{"Authorization": "Bearer root"}) //nolint:bodyclose // closed via t.Cleanup in openSSE
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "created", nextEvent(t, adminEvents, 10*time.Second, "admin initial event").Status)
}

// TestGateway_SSE_LastEventIDResume: a reconnect carrying Last-Event-ID equal
// to the current status ordinal must NOT replay the current status; a later
// transition is still delivered.
func TestGateway_SSE_LastEventIDResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	t.Setenv("REDIS_ADDRS", "")
	t.Setenv("GATEWAY_SSE_POLL_INTERVAL", "200ms")
	baseURL := startApp(t, broker, dsn)

	orderID := uuid.New().String()
	produceEvent(t, broker, topicOrdersEvents, "orders.OrderCreated.v1", orderID, func() proto.Message {
		return &ordersv1.OrderCreated{
			OrderId: orderID, CustomerId: "cust-resume", AmountCents: 700, Currency: "USD",
		}
	})
	pollOrderStatus(t, baseURL, orderID, "created", 30*time.Second)

	// Reconnect already knowing "created" (ordinal 1): no initial replay.
	events, _ := openSSE(t, baseURL, orderID, map[string]string{"Last-Event-ID": "1"}) //nolint:bodyclose // closed via t.Cleanup in openSSE
	select {
	case evt := <-events:
		t.Fatalf("expected no replay of the known status, got %+v", evt)
	case <-time.After(1 * time.Second):
	}

	// A NEWER transition still arrives.
	produceEvent(t, broker, topicPaymentsEvents, "orders.PaymentProcessed.v1", orderID, func() proto.Message {
		return &ordersv1.PaymentProcessed{
			OrderId: orderID, PaymentId: uuid.New().String(), Status: "success",
		}
	})
	require.Equal(t, "paid", nextEvent(t, events, 30*time.Second, "paid event after resume").Status)
}

// pollOrderStatusAs is pollOrderStatus with a bearer token.
func pollOrderStatusAs(t *testing.T, baseURL, orderID, expectedStatus, token string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			fmt.Sprintf("%s/v1/orders/%s", baseURL, orderID), http.NoBody)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
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
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("order %s did not reach status %q within %v", orderID, expectedStatus, timeout)
}
