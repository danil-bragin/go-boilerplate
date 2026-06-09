// FUNCTIONAL TEST TEMPLATE
//
// Scope     : a slice across several collaborators (function + cache + publisher
//   - external HTTP dependency) wired together
//
// Infra     : none — no containers, no real network sockets
// Doubles   : hand fakes (platform/testkit/fakes) for stateful in-process
//
//	collaborators; mockhttp.Server + mockhttp.JSON for external HTTP
//
// Speed     : always -short-safe; the mock HTTP server is an in-process
//
//	httptest.Server bound to 127.0.0.1
//
// When to use this pattern
// ------------------------
// Use a functional test (also called a component test) when you want to
// exercise a *slice* of the system — a whole use-case or application-layer
// function — without spinning up real infrastructure. The goal is to assert
// *behaviour* (the right data ends up in the right place) rather than
// *interactions* (exact call counts on every collaborator).
//
//	Fake vs Mock
//	─────────────────────────────────────────────────────────────────────────
//	fakes.Publisher  – stateful in-memory double; call .Messages() to inspect
//	                   what was published.  Use when you care about *state*.
//	mocks.PublisherMock – strict call recorder; use when you need to assert
//	                      exact argument values or call ordering.
//	mockhttp.Server  – wraps an httptest.Server + records requests; use for
//	                   any external HTTP dependency (REST API, JWKS, webhook).
//
// Copy this file, replace FetchAndNotify with your own flow, swap in the
// collaborators your function needs, and adjust the assertions.
package testingexamples_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// The flow under test.
	testingex "go-boilerplate/examples/testing"

	// fakes: hand-written stateful in-memory doubles.
	// Use fakes when the test cares about the state the double accumulates
	// (e.g. "was this message stored?") rather than exact call interactions.
	"go-boilerplate/platform/testkit/fakes"

	// mockhttp: in-process httptest-based mock servers.
	// mockhttp.Server wraps an http.Handler and records every request.
	// mockhttp.JSON is a one-liner factory for static JSON responses.
	// mockhttp.JWKS provides a live RS256 JWKS endpoint + JWT minter.
	"go-boilerplate/platform/testkit/mockhttp"
)

// TestFetchAndNotify_Success exercises the full FetchAndNotify slice:
//
//  1. An external HTTP GET is made to the mock server.
//  2. The returned item is stored in the fake cache under "item:<id>".
//  3. An outbox event is published via the fake publisher.
//
// FUNCTIONAL: a slice across collaborators using fakes + mock servers,
// behaviour not interactions, no real infra, always -short.
func TestFetchAndNotify_Success(t *testing.T) {
	// Arrange ----------------------------------------------------------------

	const itemID = "item-42"
	wantItem := testingex.ItemResponse{ID: itemID, Name: "Widget"}

	// Set up a mock HTTP server that will stand in for the external catalogue
	// API. mockhttp.Server wraps any http.Handler and records every request.
	// mockhttp.JSON builds a handler that returns the given value as JSON.
	//
	// The server is automatically closed when the test ends (t.Cleanup).
	recorder := mockhttp.Server(t, mockhttp.JSON(http.StatusOK, wantItem))

	// Fake cache: in-memory, concurrent-safe, TTL-agnostic.
	// After the function runs we can inspect its contents directly.
	cache := fakes.NewCache()

	// Fake publisher: in-memory, appends messages to a slice.
	// Call .Messages() to retrieve everything that was published.
	pub := fakes.NewPublisher()

	ctx := context.Background()

	// Act --------------------------------------------------------------------

	err := testingex.FetchAndNotify(ctx, recorder.URL(), itemID, cache, pub)

	// Assert -----------------------------------------------------------------

	require.NoError(t, err)

	// 1. Verify the external HTTP call was made.
	reqs := recorder.Requests()
	assert.Len(t, reqs, 1, "expected exactly one HTTP request to the mock server")
	if len(reqs) > 0 {
		assert.Equal(t, http.MethodGet, reqs[0].Method)
		assert.Equal(t, "/items/"+itemID, reqs[0].Path)
	}

	// 2. Verify the item was cached.
	raw, hit := cache.Get(ctx, "item:"+itemID)
	assert.True(t, hit, "item should be present in the cache after a successful fetch")
	if hit {
		var cached testingex.ItemResponse
		require.NoError(t, json.Unmarshal(raw, &cached))
		assert.Equal(t, wantItem.ID, cached.ID)
		assert.Equal(t, wantItem.Name, cached.Name)
	}

	// 3. Verify the outbox event was published.
	messages := pub.Messages()
	assert.Len(t, messages, 1, "expected exactly one outbox message after fetch")
	if len(messages) > 0 {
		assert.Equal(t, "catalogue.events", messages[0].AggregateType)
		assert.Equal(t, itemID, messages[0].AggregateID)
		assert.Equal(t, "ItemFetched", messages[0].EventType)
	}
}

// TestFetchAndNotify_ExternalAPIError verifies that FetchAndNotify propagates
// an error when the external API returns a non-200 status.
//
// FUNCTIONAL: use a mock server that returns an error status; assert the
// function surfaces it and neither publishes nor caches anything.
func TestFetchAndNotify_ExternalAPIError(t *testing.T) {
	// Arrange ----------------------------------------------------------------

	// The mock server will return a 503 Service Unavailable.
	recorder := mockhttp.Server(t, mockhttp.JSON(http.StatusServiceUnavailable, map[string]string{
		"error": "service temporarily unavailable",
	}))

	cache := fakes.NewCache()
	pub := fakes.NewPublisher()
	ctx := context.Background()

	// Act --------------------------------------------------------------------

	err := testingex.FetchAndNotify(ctx, recorder.URL(), "item-99", cache, pub)

	// Assert -----------------------------------------------------------------

	// The function must return an error when the upstream API is degraded.
	require.Error(t, err)

	// Nothing should have been published — the error was returned before Publish.
	assert.Empty(t, pub.Messages(),
		"no messages should be published when the external API fails")
}

// TestFetchAndNotify_PublisherFailure verifies graceful handling when the
// publisher is unavailable after a successful HTTP fetch.
//
// FUNCTIONAL: inject publisher failure via fakes.Publisher.FailNext to
// simulate a transient broker error.
func TestFetchAndNotify_PublisherFailure(t *testing.T) {
	// Arrange ----------------------------------------------------------------

	item := testingex.ItemResponse{ID: "item-7", Name: "Gizmo"}
	recorder := mockhttp.Server(t, mockhttp.JSON(http.StatusOK, item))

	cache := fakes.NewCache()
	pub := fakes.NewPublisher()

	// FailNext causes the next Publish call to return an error without storing
	// the message. This simulates a transient broker failure.
	pub.FailNext = true

	ctx := context.Background()

	// Act --------------------------------------------------------------------

	err := testingex.FetchAndNotify(ctx, recorder.URL(), item.ID, cache, pub)

	// Assert -----------------------------------------------------------------

	require.Error(t, err, "should surface the publisher error")

	// No messages should have been recorded (FailNext clears without storing).
	assert.Empty(t, pub.Messages())
}
