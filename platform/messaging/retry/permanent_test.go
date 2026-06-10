package retry_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/messaging/retry"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingProducer captures produced records in memory (no broker).
type recordingProducer struct {
	mu   sync.Mutex
	recs []kafka.Record
}

func (p *recordingProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recs = append(p.recs, rec)
	return nil
}

func (p *recordingProducer) records() []kafka.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]kafka.Record(nil), p.recs...)
}

// TestEscalate_PermanentRoutesStraightToDLT: a permanent apperr cause skips
// all remaining tiers — the record lands on the DLT with retry-last-error
// preserved and the x-error-code header carrying the apperr code.
func TestEscalate_PermanentRoutesStraightToDLT(t *testing.T) {
	t.Parallel()
	prod := &recordingProducer{}
	pol := retry.Policy{Tiers: []time.Duration{time.Second, time.Minute}}
	esc := retry.NewEscalator(prod, pol)

	cause := fmt.Errorf("handler: %w", apperr.New(apperr.CodeValidationFailed))
	rec := kafka.Record{Topic: "orders.commands", Key: []byte("k"), Value: []byte("v")}

	dest, err := esc.Escalate(context.Background(), "orders.commands", rec, cause)
	require.NoError(t, err)
	assert.Equal(t, "orders.commands.DLT", dest, "permanent error must skip every tier")

	recs := prod.records()
	require.Len(t, recs, 1)
	out := recs[0]
	assert.Equal(t, "orders.commands.DLT", out.Topic)
	assert.Contains(t, out.Headers[retry.HeaderLastError], "VALIDATION_FAILED")
	assert.Equal(t, apperr.CodeValidationFailed, out.Headers[kafka.HeaderDLTErrorCode])
}

// TestEscalate_TransientStillUsesTiers pins the unchanged transient path.
func TestEscalate_TransientStillUsesTiers(t *testing.T) {
	t.Parallel()
	prod := &recordingProducer{}
	pol := retry.Policy{Tiers: []time.Duration{time.Second, time.Minute}}
	esc := retry.NewEscalator(prod, pol)

	cause := fmt.Errorf("transient: %w", context.DeadlineExceeded)
	rec := kafka.Record{Topic: "orders.commands", Key: []byte("k"), Value: []byte("v")}

	dest, err := esc.Escalate(context.Background(), "orders.commands", rec, cause)
	require.NoError(t, err)
	assert.Equal(t, "orders.commands.retry.0", dest)
}

// TestWrapHandler_PermanentSkipsFastAttempts: the fast-attempt loop stops
// after the FIRST failure when the error is permanent.
func TestWrapHandler_PermanentSkipsFastAttempts(t *testing.T) {
	t.Parallel()
	prod := &recordingProducer{}
	pol := retry.Policy{Tiers: []time.Duration{time.Second}, FastAttempts: 4, FastBackoff: time.Millisecond}
	esc := retry.NewEscalator(prod, pol)

	var calls atomic.Int32
	handler := func(context.Context, kafka.Record) error {
		calls.Add(1)
		return apperr.New(apperr.CodeValidationFailed)
	}
	wrapped := retry.WrapHandler(handler, esc, pol)

	err := wrapped(context.Background(), kafka.Record{Topic: "t", Key: []byte("k")})
	require.NoError(t, err, "successful DLT routing commits the offset")
	assert.Equal(t, int32(1), calls.Load(), "no extra fast attempts for permanent errors")

	recs := prod.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "t.DLT", recs[0].Topic)
}

// TestTieredRetry_PermanentToDLTIntegration is the end-to-end check against
// a real broker: a handler failing with a permanent apperr leads to exactly
// one record on the DLT after exactly one attempt, and the tier topics stay
// empty.
func TestTieredRetry_PermanentToDLTIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)
	ctx := context.Background()

	base := "perm." + uuid.NewString()[:8]
	pol := retry.Policy{Tiers: []time.Duration{2 * time.Second, time.Minute}, FastAttempts: 3, FastBackoff: time.Millisecond}
	_, _, esc := setupIntegration(t, broker, base, pol)

	var calls atomic.Int32
	handler := func(context.Context, kafka.Record) error {
		calls.Add(1)
		return fmt.Errorf("validate: %w", apperr.New(apperr.CodeValidationFailed))
	}
	wrapped := retry.WrapHandler(handler, esc, pol)

	rec := kafka.Record{Topic: base, Key: []byte("k1"), Value: []byte("payload")}
	require.NoError(t, wrapped(ctx, rec), "DLT routing must commit")
	assert.Equal(t, int32(1), calls.Load(), "exactly one attempt")

	dlt := pollKGO(t, broker, retry.DLTTopic(base), 1, 30*time.Second)
	require.Len(t, dlt, 1, "exactly one DLT record")
	assert.Equal(t, []byte("payload"), dlt[0].Value)
	assert.Equal(t, apperr.CodeValidationFailed, kgoHeaderVal(dlt[0], kafka.HeaderDLTErrorCode))
	assert.NotEmpty(t, kgoHeaderVal(dlt[0], retry.HeaderLastError))

	for i := range pol.Tiers {
		tier := pollKGO(t, broker, retry.TierTopic(base, i), 1, 2*time.Second)
		assert.Empty(t, tier, "tier %d must stay empty for permanent errors", i)
	}
}
