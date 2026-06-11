package contract_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go-boilerplate/examples/e2e/contract"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// TestEventTypeNames_WireFrozen pins the LITERAL wire name of every event
// that crosses a service boundary, and that the produce-side record header
// carries exactly that name. Producers and consumers both derive the name
// via consume.EventTypeFor from the shared proto type, so they cannot drift
// from each other — but a proto message/package rename would silently change
// the derived name on BOTH sides at once, breaking compatibility with
// records already in Kafka. These literals make that rename loud.
func TestEventTypeNames_WireFrozen(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	fixtures := map[string]outbox.Message{
		"orders.OrderCreated.v1":         contract.OrderCreated(t, "o-1", "cust-1", 100, "USD"),
		"orders.OrderPaymentTimedOut.v1": contract.OrderPaymentTimedOut(t, "o-1", now),
		"orders.PaymentProcessed.v1":     contract.PaymentProcessed(t, "o-1", "p-1"),
		"orders.PaymentFailed.v1":        contract.PaymentFailed(t, "o-1", "declined", now),
	}
	for wireName, msg := range fixtures {
		assert.Equal(t, wireName, msg.EventType, "fixture event type")
		rec := contract.WireRecord(t, msg)
		assert.Equal(t, wireName, rec.Headers[kafka.HeaderEventType],
			"produce-side event-type header for %s", wireName)
		assert.Equal(t, msg.ID.String(), rec.Headers[kafka.HeaderMessageID],
			"message-id header must carry the outbox row id (inbox dedup identity)")
		assert.Equal(t, []byte(msg.AggregateID), rec.Key,
			"record key must be the aggregate id (per-aggregate ordering)")
	}
}

// TestTopics_WireFrozen pins the literal topic names the fixtures ride on —
// the services' config defaults assert against the same constants in their
// own contract tests.
func TestTopics_WireFrozen(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "orders.events", contract.OrdersEventsTopic)
	assert.Equal(t, "payments.events", contract.PaymentsEventsTopic)
	assert.Equal(t, contract.OrdersEventsTopic, contract.OrderCreated(t, "o", "c", 1, "USD").Topic)
	assert.Equal(t, contract.PaymentsEventsTopic, contract.PaymentProcessed(t, "o", "p").Topic)
}

// TestChainAndPrincipalHeaders_SurviveRoundtrip drives the producer-side
// metadata path (auth.InjectHeaders + outbox.StampChainHeaders — exactly what
// the gateway edge and outbox.Enqueue do) through the REAL outboxkafka record
// build and the REAL consume pipeline, asserting the consumer-side handler
// observes the same chain lineage and principal:
//
//   - correlation-id propagates verbatim (chain-constant);
//   - causation-id is stamped from the producer's parent message id, and the
//     CONSUMER's ctx gets parent = THIS record's message id (next hop's
//     causation);
//   - principal-sub / principal-roles survive into auth.From(ctx), roles
//     losslessly (JSON-encoded — a role containing a comma must not split).
func TestChainAndPrincipalHeaders_SurviveRoundtrip(t *testing.T) {
	t.Parallel()

	// Producer side: a handler mid-chain, acting for an authenticated user.
	prodCtx := msgctx.WithCorrelationID(context.Background(), "chain-root-42")
	prodCtx = msgctx.WithParentMessageID(prodCtx, "parent-msg-7")
	principal := auth.Principal{Subject: "user-9", Roles: []string{"admin", "ops, sre"}}
	prodCtx = auth.Into(prodCtx, principal)

	msg := contract.OrderCreated(t, "1f0c30f4-9f7a-4a4a-8b3e-2d1a5c6e7f80", "cust-1", 500, "EUR")
	custom := map[string]string{}
	auth.InjectHeaders(prodCtx, custom) // what edge producers do for actor attribution
	hdrs, err := json.Marshal(custom)
	require.NoError(t, err)
	msg.Headers = hdrs
	msg = outbox.StampChainHeaders(prodCtx, msg) // what outbox.Enqueue persists

	// Wire: real record build (headers merged) → in-memory broker.
	rec := contract.WireRecord(t, msg)
	assert.Equal(t, "chain-root-42", rec.Headers[msgctx.HeaderCorrelationID])
	assert.Equal(t, "parent-msg-7", rec.Headers[msgctx.HeaderCausationID])
	assert.Equal(t, "user-9", rec.Headers[auth.HeaderPrincipalSub])

	// Consumer side: the real consume pipeline (WithoutInbox — the documented
	// test-only escape; decode/dispatch/metadata installation are identical).
	type seen struct {
		corr, parent string
		principal    auth.Principal
		ok           bool
	}
	var got seen
	handler := consume.New(nil, "contract-roundtrip", consume.WithoutInbox()).Handler(
		consume.TypedFor(1, func(ctx context.Context, _ *ordersv1.OrderCreated) error {
			p, ok := auth.From(ctx)
			got = seen{
				corr:      msgctx.CorrelationID(ctx),
				parent:    msgctx.ParentMessageID(ctx),
				principal: p,
				ok:        ok,
			}
			return nil
		}),
	)
	broker := fakes.NewBroker()
	broker.Subscribe(msg.Topic, handler)
	require.NoError(t, broker.Produce(context.Background(), rec))

	assert.Equal(t, "chain-root-42", got.corr, "correlation id must propagate verbatim")
	assert.Equal(t, msg.ID.String(), got.parent,
		"consumer's causation parent must be THIS record's message id")
	require.True(t, got.ok, "principal must be installed from the propagation headers")
	assert.Equal(t, principal, got.principal,
		"principal subject + roles must round-trip losslessly (incl. comma-in-role)")
}
