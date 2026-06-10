package retry_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parkProducer captures produced records (thread-safe).
type parkProducer struct {
	mu   sync.Mutex
	recs []kafka.Record
	err  error
}

func (p *parkProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.recs = append(p.recs, rec)
	return nil
}

func (p *parkProducer) records() []kafka.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]kafka.Record, len(p.recs))
	copy(out, p.recs)
	return out
}

func baseRec(key, val string) kafka.Record {
	return kafka.Record{
		Topic: "orders.commands",
		Key:   []byte(key),
		Value: []byte(val),
	}
}

// TestKeyParking_EscalateParksKey: after a real escalation, the same key on
// the same base topic is reported parked; other keys are not.
func TestKeyParking_EscalateParksKey(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.DefaultPolicy()
	esc := retry.NewEscalator(prod, pol, retry.WithKeyParking(time.Hour))

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.NoError(t, err)

	assert.True(t, esc.KeyParked("orders.commands", []byte("K1")), "escalated key must be parked")
	assert.False(t, esc.KeyParked("orders.commands", []byte("K2")), "other keys must not be parked")
	assert.False(t, esc.KeyParked("payments.events", []byte("K1")), "same key on another topic must not be parked")
}

// TestKeyParking_WindowExpires: a parked key flows normally after the window.
func TestKeyParking_WindowExpires(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.DefaultPolicy()
	esc := retry.NewEscalator(prod, pol, retry.WithKeyParking(30*time.Millisecond))

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.NoError(t, err)
	require.True(t, esc.KeyParked("orders.commands", []byte("K1")))

	time.Sleep(50 * time.Millisecond)
	assert.False(t, esc.KeyParked("orders.commands", []byte("K1")), "parking must expire after window")
}

// TestKeyParking_PolicyFieldEnablesParking: setting Policy.KeyParkingWindow
// alone (no WithKeyParking option) must activate parking — NewEscalator
// honors the policy field.
func TestKeyParking_PolicyFieldEnablesParking(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.DefaultPolicy()
	pol.KeyParkingWindow = time.Hour
	esc := retry.NewEscalator(prod, pol)

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.NoError(t, err)
	assert.True(t, esc.KeyParked("orders.commands", []byte("K1")),
		"Policy.KeyParkingWindow alone must enable parking")
}

// TestKeyParking_OptionOverridesPolicyField: WithKeyParking takes precedence
// over Policy.KeyParkingWindow.
func TestKeyParking_OptionOverridesPolicyField(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.DefaultPolicy()
	pol.KeyParkingWindow = time.Hour
	esc := retry.NewEscalator(prod, pol, retry.WithKeyParking(time.Nanosecond))

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.NoError(t, err)
	time.Sleep(time.Millisecond)
	assert.False(t, esc.KeyParked("orders.commands", []byte("K1")),
		"WithKeyParking(1ns) must override the policy's 1h window")
}

// TestKeyParking_DisabledByDefault: without WithKeyParking, KeyParked is
// always false.
func TestKeyParking_DisabledByDefault(t *testing.T) {
	prod := &parkProducer{}
	esc := retry.NewEscalator(prod, retry.DefaultPolicy())

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.NoError(t, err)
	assert.False(t, esc.KeyParked("orders.commands", []byte("K1")))
}

// TestKeyParking_FailedEscalationDoesNotPark: a failed escalation produce must
// not park the key — the record was not diverted anywhere.
func TestKeyParking_FailedEscalationDoesNotPark(t *testing.T) {
	prod := &parkProducer{err: errors.New("broker down")}
	esc := retry.NewEscalator(prod, retry.DefaultPolicy(), retry.WithKeyParking(time.Hour))

	_, err := esc.Escalate(context.Background(), "orders.commands", baseRec("K1", "v1"), errors.New("boom"))
	require.Error(t, err)
	assert.False(t, esc.KeyParked("orders.commands", []byte("K1")))
}

// TestKeyParking_DivertGoesToFirstTier: Divert publishes the record to the
// first retry tier with attempt=1 and a due-at of now+Tiers[0], and does NOT
// extend the parking window (only real escalations park).
func TestKeyParking_DivertGoesToFirstTier(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.DefaultPolicy()
	esc := retry.NewEscalator(prod, pol, retry.WithKeyParking(time.Hour))

	now := time.Now()
	dest, err := esc.Divert(context.Background(), "orders.commands", baseRec("K1", "v2"))
	require.NoError(t, err)
	assert.Equal(t, retry.TierTopic("orders.commands", 0), dest)

	recs := prod.records()
	require.Len(t, recs, 1)
	assert.Equal(t, dest, recs[0].Topic)
	attempt, orig, due, ok := retry.ParseRetryHeaders(recs[0])
	require.True(t, ok, "diverted record must carry retry headers")
	assert.Equal(t, 1, attempt)
	assert.Equal(t, "orders.commands", orig)
	assert.WithinDuration(t, now.Add(pol.Tiers[0]), due, 2*time.Second)

	assert.False(t, esc.KeyParked("orders.commands", []byte("K1")),
		"Divert must not park/extend — only real escalations do")
}

// TestWrapHandler_ParkedKeyDivertsWithoutHandler verifies the consumer-side
// wrap: K1 fails → escalates → the NEXT record with key K1 is diverted to the
// first tier WITHOUT invoking the handler; K2 is unaffected; after the window
// K1 flows normally again.
func TestWrapHandler_ParkedKeyDivertsWithoutHandler(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.Policy{
		Tiers:            []time.Duration{5 * time.Second},
		FastAttempts:     1,
		KeyParkingWindow: 80 * time.Millisecond,
	}
	esc := retry.NewEscalator(prod, pol, retry.WithKeyParking(pol.KeyParkingWindow))

	var mu sync.Mutex
	seen := map[string]int{} // value → handler invocations
	failFirstK1 := true
	handler := func(_ context.Context, r kafka.Record) error {
		mu.Lock()
		defer mu.Unlock()
		seen[string(r.Value)]++
		if string(r.Key) == "K1" && failFirstK1 {
			failFirstK1 = false
			return errors.New("transient failure on K1")
		}
		return nil
	}

	wrapped := retry.WrapHandler(handler, esc, pol)
	ctx := context.Background()

	// 1. K1 first record fails → escalated to tier 0, key parked.
	require.NoError(t, wrapped(ctx, baseRec("K1", "k1-rec-1")))
	require.Len(t, prod.records(), 1, "first K1 record must be escalated")

	// 2. K1 second record arrives while parked → diverted, handler NOT called.
	require.NoError(t, wrapped(ctx, baseRec("K1", "k1-rec-2")))
	mu.Lock()
	assert.Zero(t, seen["k1-rec-2"], "handler must never see a parked key's record")
	mu.Unlock()
	recs := prod.records()
	require.Len(t, recs, 2, "second K1 record must be diverted to the retry tier")
	assert.Equal(t, retry.TierTopic("orders.commands", 0), recs[1].Topic)
	assert.Equal(t, []byte("k1-rec-2"), recs[1].Value)

	// 3. K2 is unaffected: handler processes it normally, nothing produced.
	require.NoError(t, wrapped(ctx, baseRec("K2", "k2-rec-1")))
	mu.Lock()
	assert.Equal(t, 1, seen["k2-rec-1"], "other keys must flow through the handler")
	mu.Unlock()
	require.Len(t, prod.records(), 2)

	// 4. After the window expires K1 flows normally again.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, wrapped(ctx, baseRec("K1", "k1-rec-3")))
	mu.Lock()
	assert.Equal(t, 1, seen["k1-rec-3"], "expired parking must let the key flow")
	mu.Unlock()
	require.Len(t, prod.records(), 2, "no new escalation/diversion after expiry")
}

// TestWrapHandler_FastAttemptsThenEscalate verifies the fast-attempt loop:
// FastAttempts in-process attempts, then escalation (and offset commit).
func TestWrapHandler_FastAttemptsThenEscalate(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.Policy{Tiers: []time.Duration{5 * time.Second}, FastAttempts: 3}
	esc := retry.NewEscalator(prod, pol)

	var calls int
	handler := func(_ context.Context, _ kafka.Record) error {
		calls++
		return errors.New("always fails")
	}

	wrapped := retry.WrapHandler(handler, esc, pol)
	require.NoError(t, wrapped(context.Background(), baseRec("K1", "v")),
		"successful escalation must return nil so the offset is committed")
	assert.Equal(t, 3, calls, "handler must run FastAttempts times")
	require.Len(t, prod.records(), 1)
	assert.Equal(t, retry.TierTopic("orders.commands", 0), prod.records()[0].Topic)
}

// TestWrapHandler_EscalationFailurePropagates: when the escalation produce
// fails the wrapper must return the error (record NOT committed → redelivered).
func TestWrapHandler_EscalationFailurePropagates(t *testing.T) {
	prod := &parkProducer{err: errors.New("broker down")}
	pol := retry.Policy{Tiers: []time.Duration{5 * time.Second}, FastAttempts: 1}
	esc := retry.NewEscalator(prod, pol)

	handler := func(_ context.Context, _ kafka.Record) error { return errors.New("boom") }
	wrapped := retry.WrapHandler(handler, esc, pol)
	require.Error(t, wrapped(context.Background(), baseRec("K1", "v")))
}

// TestKeyParking_ManyKeysPruned exercises lazy pruning indirectly through the
// public API: park many keys with a tiny window, let them expire, then park
// one more and verify expired keys all read as un-parked.
func TestKeyParking_ManyKeysPruned(t *testing.T) {
	prod := &parkProducer{}
	esc := retry.NewEscalator(prod, retry.DefaultPolicy(), retry.WithKeyParking(10*time.Millisecond))

	ctx := context.Background()
	for i := 0; i < 300; i++ {
		_, err := esc.Escalate(ctx, "orders.commands", baseRec("K"+strconv.Itoa(i), "v"), errors.New("x"))
		require.NoError(t, err)
	}
	time.Sleep(30 * time.Millisecond)
	for i := 0; i < 300; i++ {
		assert.False(t, esc.KeyParked("orders.commands", []byte("K"+strconv.Itoa(i))))
	}
}
