package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefaultPolicy_FastBackoff: the recommended policy carries the 100ms
// fast-attempt backoff explicitly.
func TestDefaultPolicy_FastBackoff(t *testing.T) {
	assert.Equal(t, 100*time.Millisecond, retry.DefaultPolicy().FastBackoff)
}

// TestWrapHandler_UsesPolicyFastBackoff: with FastAttempts=2 the wrapper
// sleeps FastBackoff between the in-process attempts, so total elapsed time
// is at least one backoff interval.
func TestWrapHandler_UsesPolicyFastBackoff(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.Policy{
		Tiers:        []time.Duration{time.Second},
		FastAttempts: 2,
		FastBackoff:  150 * time.Millisecond,
	}
	esc := retry.NewEscalator(prod, pol)
	h := retry.WrapHandler(func(context.Context, kafka.Record) error {
		return errors.New("boom")
	}, esc, pol)

	start := time.Now()
	require.NoError(t, h(context.Background(), baseRec("K1", "v1")))
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond,
		"WrapHandler must sleep Policy.FastBackoff between fast attempts")
}

// TestWrapHandler_FastBackoffDefault: an unset (zero) FastBackoff falls back
// to 100ms — preserving the pre-knob behavior.
func TestWrapHandler_FastBackoffDefault(t *testing.T) {
	prod := &parkProducer{}
	pol := retry.Policy{
		Tiers:        []time.Duration{time.Second},
		FastAttempts: 2, // FastBackoff intentionally zero
	}
	esc := retry.NewEscalator(prod, pol)
	h := retry.WrapHandler(func(context.Context, kafka.Record) error {
		return errors.New("boom")
	}, esc, pol)

	start := time.Now()
	require.NoError(t, h(context.Background(), baseRec("K1", "v1")))
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond,
		"zero FastBackoff must default to 100ms")
}
