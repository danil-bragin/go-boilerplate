package retry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureProducer is a fake producer that captures produced records.
type captureProducer struct {
	recs []kafka.Record
	err  error
}

func (c *captureProducer) Produce(_ context.Context, rec kafka.Record) error {
	if c.err != nil {
		return c.err
	}
	c.recs = append(c.recs, rec)
	return nil
}

// ---------------------------------------------------------------------------
// TierTopic naming
// ---------------------------------------------------------------------------

func TestTierTopic(t *testing.T) {
	tests := []struct {
		base string
		idx  int
		want string
	}{
		{"orders.commands", 0, "orders.commands.retry.0"},
		{"orders.commands", 1, "orders.commands.retry.1"},
		{"orders.commands", 2, "orders.commands.retry.2"},
		{"payments.events", 10, "payments.events.retry.10"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := retry.TierTopic(tc.base, tc.idx)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// DLTTopic — must match dlq.go convention (base + ".DLT")
// ---------------------------------------------------------------------------

func TestDLTTopic(t *testing.T) {
	assert.Equal(t, "orders.commands.DLT", retry.DLTTopic("orders.commands"))
	assert.Equal(t, "payments.DLT", retry.DLTTopic("payments"))
}

// ---------------------------------------------------------------------------
// SetRetryHeaders / ParseRetryHeaders round-trip
// ---------------------------------------------------------------------------

func TestSetParseRetryHeaders_RoundTrip(t *testing.T) {
	rec := kafka.Record{
		Topic:   "orders.commands",
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: make(map[string]string),
	}
	due := time.Now().Add(5 * time.Second).Truncate(time.Millisecond)
	retry.SetRetryHeaders(&rec, 1, "orders.commands", due, errors.New("boom"))

	attempt, origTopic, parsedDue, ok := retry.ParseRetryHeaders(rec)
	require.True(t, ok)
	assert.Equal(t, 1, attempt)
	assert.Equal(t, "orders.commands", origTopic)
	assert.Equal(t, due.UnixMilli(), parsedDue.UnixMilli())
}

func TestParseRetryHeaders_MissingAttempt(t *testing.T) {
	rec := kafka.Record{
		Headers: map[string]string{
			retry.HeaderOrigTopic: "orders.commands",
			retry.HeaderDueAt:     "1234567890000",
		},
	}
	_, _, _, ok := retry.ParseRetryHeaders(rec)
	assert.False(t, ok, "missing attempt header should return ok=false")
}

func TestParseRetryHeaders_MissingOrigTopic(t *testing.T) {
	rec := kafka.Record{
		Headers: map[string]string{
			retry.HeaderAttempt: "1",
			retry.HeaderDueAt:   "1234567890000",
		},
	}
	_, _, _, ok := retry.ParseRetryHeaders(rec)
	assert.False(t, ok, "missing orig-topic header should return ok=false")
}

func TestParseRetryHeaders_MissingDueAt(t *testing.T) {
	rec := kafka.Record{
		Headers: map[string]string{
			retry.HeaderAttempt:   "1",
			retry.HeaderOrigTopic: "orders.commands",
		},
	}
	_, _, _, ok := retry.ParseRetryHeaders(rec)
	assert.False(t, ok, "missing due-at header should return ok=false")
}

func TestParseRetryHeaders_MalformedDueAt(t *testing.T) {
	rec := kafka.Record{
		Headers: map[string]string{
			retry.HeaderAttempt:   "1",
			retry.HeaderOrigTopic: "orders.commands",
			retry.HeaderDueAt:     "not-a-number",
		},
	}
	_, _, _, ok := retry.ParseRetryHeaders(rec)
	assert.False(t, ok, "malformed due-at header should return ok=false")
}

func TestParseRetryHeaders_MalformedAttempt(t *testing.T) {
	rec := kafka.Record{
		Headers: map[string]string{
			retry.HeaderAttempt:   "nope",
			retry.HeaderOrigTopic: "orders.commands",
			retry.HeaderDueAt:     "1234567890000",
		},
	}
	_, _, _, ok := retry.ParseRetryHeaders(rec)
	assert.False(t, ok, "malformed attempt header should return ok=false")
}

func TestSetRetryHeaders_ErrorTruncatedAt512(t *testing.T) {
	longErr := errors.New(strings.Repeat("x", 600))
	rec := kafka.Record{Headers: make(map[string]string)}
	retry.SetRetryHeaders(&rec, 1, "base", time.Now(), longErr)

	lastError := rec.Headers[retry.HeaderLastError]
	assert.LessOrEqual(t, len(lastError), 512, "error header must be truncated to 512 bytes")
	assert.Equal(t, 512, len(lastError), "error should be exactly 512 bytes when the original exceeds 512")
}

// ---------------------------------------------------------------------------
// DefaultPolicy
// ---------------------------------------------------------------------------

func TestDefaultPolicy(t *testing.T) {
	p := retry.DefaultPolicy()
	assert.Equal(t, []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}, p.Tiers)
	assert.Equal(t, 1, p.FastAttempts)
}

// ---------------------------------------------------------------------------
// Escalator — first failure (no retry headers on record)
// ---------------------------------------------------------------------------

func TestEscalator_FirstFailure_RoutesToTier0(t *testing.T) {
	prod := &captureProducer{}
	pol := retry.DefaultPolicy()
	esc := retry.NewEscalator(prod, pol)

	origRec := kafka.Record{
		Topic:   "orders.commands",
		Key:     []byte("mykey"),
		Value:   []byte("myval"),
		Headers: map[string]string{"x-trace": "abc"},
	}

	now := time.Now()
	dest, err := esc.Escalate(context.Background(), "orders.commands", origRec, errors.New("fail"))
	require.NoError(t, err)

	// destination must be tier-0 topic
	assert.Equal(t, retry.TierTopic("orders.commands", 0), dest)
	require.Len(t, prod.recs, 1)

	produced := prod.recs[0]
	assert.Equal(t, dest, produced.Topic)
	assert.Equal(t, []byte("mykey"), produced.Key)
	assert.Equal(t, []byte("myval"), produced.Value)

	// attempt header must be "1" (first escalation done)
	assert.Equal(t, "1", produced.Headers[retry.HeaderAttempt])
	// orig topic preserved
	assert.Equal(t, "orders.commands", produced.Headers[retry.HeaderOrigTopic])
	// original user headers preserved
	assert.Equal(t, "abc", produced.Headers["x-trace"])

	// due ≈ now + 5s (allow 2s delta for slow machines)
	attempt, _, due, ok := retry.ParseRetryHeaders(produced)
	require.True(t, ok)
	assert.Equal(t, 1, attempt)
	assert.WithinDuration(t, now.Add(pol.Tiers[0]), due, 2*time.Second)
}

// ---------------------------------------------------------------------------
// Escalator — record already in tier-1 (attempt header = "1") → tier-1 topic
// ---------------------------------------------------------------------------

func TestEscalator_Tier1Record_RoutesToTier1(t *testing.T) {
	prod := &captureProducer{}
	pol := retry.DefaultPolicy()
	esc := retry.NewEscalator(prod, pol)

	rec := kafka.Record{
		Topic:   retry.TierTopic("orders.commands", 0),
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: make(map[string]string),
	}
	// attempt=1 means 1 escalation has been done; next is tier index 1
	retry.SetRetryHeaders(&rec, 1, "orders.commands", time.Now().Add(-5*time.Second), errors.New("prev"))

	dest, err := esc.Escalate(context.Background(), "orders.commands", rec, errors.New("fail again"))
	require.NoError(t, err)
	assert.Equal(t, retry.TierTopic("orders.commands", 1), dest)

	produced := prod.recs[0]
	assert.Equal(t, "2", produced.Headers[retry.HeaderAttempt])
}

// ---------------------------------------------------------------------------
// Escalator — attempt == len(Tiers) → DLT
// ---------------------------------------------------------------------------

func TestEscalator_FinalTier_RoutesToDLT(t *testing.T) {
	prod := &captureProducer{}
	pol := retry.DefaultPolicy() // 3 tiers
	esc := retry.NewEscalator(prod, pol)

	rec := kafka.Record{
		Topic:   retry.TierTopic("orders.commands", 2),
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: make(map[string]string),
	}
	// attempt=3 means all 3 tiers done; next = DLT
	retry.SetRetryHeaders(&rec, 3, "orders.commands", time.Now().Add(-5*time.Minute), errors.New("last"))

	dest, err := esc.Escalate(context.Background(), "orders.commands", rec, errors.New("fatal"))
	require.NoError(t, err)

	assert.Equal(t, retry.DLTTopic("orders.commands"), dest)
	assert.Equal(t, dest, prod.recs[0].Topic)
}

// ---------------------------------------------------------------------------
// Escalator — producer error propagates
// ---------------------------------------------------------------------------

func TestEscalator_ProducerError_Propagates(t *testing.T) {
	prod := &captureProducer{err: errors.New("broker down")}
	esc := retry.NewEscalator(prod, retry.DefaultPolicy())

	rec := kafka.Record{
		Topic:   "orders.commands",
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: make(map[string]string),
	}

	_, err := esc.Escalate(context.Background(), "orders.commands", rec, errors.New("fail"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broker down")
}

// ---------------------------------------------------------------------------
// Escalator — full walk: no headers → tier-0 → tier-1 → tier-2 → DLT
// ---------------------------------------------------------------------------

func TestEscalator_FullWalk(t *testing.T) {
	pol := retry.DefaultPolicy()

	type step struct {
		inAttempt   int
		wantDest    string
		wantAttempt string
	}

	steps := []step{
		// no headers (attempt=0 inferred) → tier-0, attempt becomes 1
		{0, retry.TierTopic("base", 0), "1"},
		// attempt=1 → tier-1, attempt becomes 2
		{1, retry.TierTopic("base", 1), "2"},
		// attempt=2 → tier-2, attempt becomes 3
		{2, retry.TierTopic("base", 2), "3"},
		// attempt=3 (== len(Tiers)) → DLT
		{3, retry.DLTTopic("base"), "3"},
	}

	for _, s := range steps {
		prod := &captureProducer{}
		esc := retry.NewEscalator(prod, pol)

		rec := kafka.Record{
			Topic:   "base",
			Key:     []byte("k"),
			Value:   []byte("v"),
			Headers: make(map[string]string),
		}
		if s.inAttempt > 0 {
			retry.SetRetryHeaders(&rec, s.inAttempt, "base", time.Now(), errors.New("prev"))
		}

		dest, err := esc.Escalate(context.Background(), "base", rec, errors.New("err"))
		require.NoError(t, err)
		assert.Equal(t, s.wantDest, dest, "inAttempt=%d", s.inAttempt)
		assert.Equal(t, s.wantAttempt, prod.recs[0].Headers[retry.HeaderAttempt], "inAttempt=%d", s.inAttempt)
	}
}
