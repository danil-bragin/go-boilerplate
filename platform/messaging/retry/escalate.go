package retry

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/messaging/kafka"
)

// producer is the minimal interface the Escalator requires.
// kafka.Producer satisfies it via its Produce method.
type producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// Escalator publishes failed records to the next retry tier, or to the DLT
// after the final tier, preserving key/value/original headers. The caller
// commits the original record afterwards — the main partition never blocks.
type Escalator struct {
	producer producer
	policy   Policy
}

// NewEscalator constructs an Escalator backed by the given producer and policy.
func NewEscalator(p producer, policy Policy) *Escalator {
	return &Escalator{producer: p, policy: policy}
}

// Escalate routes rec (which failed with cause) to its next destination and
// returns the destination topic. attempt is read from rec's retry headers
// (0 when absent, i.e. first failure on the base topic).
//
// Routing logic:
//   - If attempt >= len(policy.Tiers) → destination is DLTTopic(origTopic).
//   - Otherwise → destination is TierTopic(origTopic, policy.Tiers[attempt]).
//
// The record is published with all original headers preserved plus the four
// retry headers. The caller must commit the original record after Escalate
// returns nil.
func (e *Escalator) Escalate(ctx context.Context, origTopic string, rec kafka.Record, cause error) (string, error) {
	attempt, _, _, ok := ParseRetryHeaders(rec)
	if !ok {
		attempt = 0
	}

	// Build the destination record with a copy of all headers.
	dest := e.nextDestination(origTopic, attempt)
	out := kafka.Record{
		Topic:   dest,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: copyHeaders(rec.Headers),
	}

	if attempt >= len(e.policy.Tiers) {
		// Final tier exhausted — route to DLT. Keep the existing retry
		// headers for forensics and add/overwrite last-error.
		out.Headers[HeaderLastError] = truncate(cause.Error(), 512)
	} else {
		due := time.Now().Add(e.policy.Tiers[attempt])
		SetRetryHeaders(&out, attempt+1, origTopic, due, cause)
	}

	if err := e.producer.Produce(ctx, out); err != nil {
		return "", fmt.Errorf("retry: escalate to %s: %w", dest, err)
	}

	return dest, nil
}

// nextDestination returns the topic the record should be sent to.
func (e *Escalator) nextDestination(origTopic string, attempt int) string {
	if attempt >= len(e.policy.Tiers) {
		return DLTTopic(origTopic)
	}
	return TierTopic(origTopic, e.policy.Tiers[attempt])
}

// copyHeaders returns a shallow copy of h; handles nil gracefully.
func copyHeaders(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

// truncate returns s capped at maxBytes bytes, respecting UTF-8 boundaries.
func truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	b := []byte(s)[:maxBytes]
	for len(b) > 0 && b[len(b)-1]&0xC0 == 0x80 {
		b = b[:len(b)-1]
	}
	return string(b)
}
