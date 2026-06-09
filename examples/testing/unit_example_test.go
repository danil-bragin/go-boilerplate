// UNIT TEST TEMPLATE
//
// Scope     : a single function / method in isolation
// Infra     : none — zero I/O, zero network, zero containers
// Doubles   : moq-generated mocks (platform/testkit/mocks) for strict
//
//	call/interaction assertions
//
// Test data : platform/testkit/fixtures for canonical builders
// Speed     : always -short-safe; runs in microseconds
//
// When to use this pattern
// ------------------------
// Use a unit test whenever you need to verify that a function:
//   - calls its collaborators with exactly the right arguments
//   - forwards collaborator errors to the caller
//   - applies branching / domain logic without touching I/O
//
// Copy this file, rename the package + function, swap in your own
// collaborator mocks, and you're done.
package testingexamples_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// The function under test lives in the same examples/testing package.
	testingex "go-boilerplate/examples/testing"

	// fixtures: canonical test-data builders with functional options.
	// Always prefer fixtures over hand-crafted structs — they have sensible
	// defaults so your test only declares what it cares about.
	"go-boilerplate/platform/testkit/fixtures"

	// mocks: moq-generated, strict call recording.
	// Use these when you need to assert *how* a collaborator was called
	// (argument values, call count, call order).
	// See platform/testkit/mocks/gen.go for the go:generate directives.
	"go-boilerplate/platform/testkit/mocks"

	"go-boilerplate/platform/outbox"
)

// TestPublishEvent_Success verifies the happy-path: PublishEvent delegates to
// the Publisher and the Publisher receives the exact message that was passed in.
//
// UNIT: smallest scope — mock collaborators, assert interactions, zero I/O,
// always runs under -short.
func TestPublishEvent_Success(t *testing.T) {
	// Arrange ----------------------------------------------------------------

	// Build the message we will publish.
	// fixtures.OutboxMessage supplies sane defaults; only override what matters
	// for this test (AggregateType and EventType).
	want := fixtures.OutboxMessage(
		fixtures.WithAggregateType("orders.events"),
		fixtures.WithEventType("Created"),
	)

	// Wire the mock publisher.
	// PublishFunc is invoked instead of any real implementation.
	// The mock records every call so we can assert on them below.
	pub := &mocks.PublisherMock{
		PublishFunc: func(_ context.Context, _ outbox.Message) error {
			// Return nil to simulate a successful publish.
			return nil
		},
	}

	ctx := context.Background()

	// Act --------------------------------------------------------------------

	err := testingex.PublishEvent(ctx, pub, want)

	// Assert -----------------------------------------------------------------

	// The function must not return an error on the happy path.
	require.NoError(t, err)

	// PublishCalls() returns a typed slice — one entry per Publish invocation.
	// We assert exactly one call was made.
	calls := pub.PublishCalls()
	assert.Len(t, calls, 1, "Publisher.Publish should be called exactly once")

	// Assert the correct message was forwarded.
	// We only compare the fields we set; the rest (ID, Payload, …) are
	// defaults from the fixture and not relevant to this test.
	assert.Equal(t, want.AggregateType, calls[0].Msg.AggregateType)
	assert.Equal(t, want.EventType, calls[0].Msg.EventType)
}

// TestPublishEvent_PublisherError verifies that PublishEvent surfaces errors
// from the Publisher wrapped with context information.
//
// UNIT: error path — configure the mock to fail, assert the caller surfaces
// the error without swallowing or mangling it.
func TestPublishEvent_PublisherError(t *testing.T) {
	// Arrange ----------------------------------------------------------------

	injectedErr := errors.New("broker unavailable")

	// Configure the mock to return an error on every call.
	pub := &mocks.PublisherMock{
		PublishFunc: func(_ context.Context, _ outbox.Message) error {
			// Simulate a broker / network failure.
			return injectedErr
		},
	}

	msg := fixtures.OutboxMessage()
	ctx := context.Background()

	// Act --------------------------------------------------------------------

	err := testingex.PublishEvent(ctx, pub, msg)

	// Assert -----------------------------------------------------------------

	// The error must be non-nil and must wrap the injected sentinel error so
	// callers can use errors.Is for targeted handling.
	require.Error(t, err)
	assert.ErrorIs(t, err, injectedErr,
		"PublishEvent should wrap and propagate the publisher error")

	// The mock should have recorded one call — the function attempted to
	// publish before the error was returned.
	assert.Len(t, pub.PublishCalls(), 1)
}
