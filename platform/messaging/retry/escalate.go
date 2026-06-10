package retry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/messaging/kafka"
)

// producer is the minimal interface the Escalator requires.
// kafka.Producer satisfies it via its Produce method.
type producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// parkKey identifies a parked key: base topic + record key.
type parkKey struct {
	topic string
	key   string
}

// parkSweepEvery bounds lazy pruning: every parkSweepEvery insertions the
// whole parked map is swept for expired entries (in addition to the per-key
// expiry check in KeyParked).
const parkSweepEvery = 256

// Escalator publishes failed records to the next retry tier, or to the DLT
// after the final tier, preserving key/value/original headers. The caller
// commits the original record afterwards — the main partition never blocks.
//
// With WithKeyParking enabled the Escalator additionally remembers which
// keys were recently escalated so the consumer wrap (WrapHandler) can divert
// follow-up records of the same key straight to the first retry tier,
// preserving per-key order (see the package-level ORDERING WARNING).
type Escalator struct {
	producer producer
	policy   Policy

	// Key-parking state (nil window ⇒ disabled). Guarded by parkMu.
	parkWindow time.Duration
	parkMu     sync.Mutex
	parked     map[parkKey]time.Time // key → parked-until
	parkOps    int                   // insertions since last sweep
}

// EscalatorOption configures optional Escalator behaviour.
type EscalatorOption func(*Escalator)

// WithKeyParking enables opt-in key parking: every successful escalation
// parks the record's key for the given window. While parked, KeyParked
// reports true for that (base topic, key) pair, and WrapHandler diverts
// matching records straight to the first retry tier without invoking the
// handler — preserving per-key order through the retry pipeline.
//
// BEST-EFFORT: the parking table is in-memory and mutex-guarded; it is lost
// on process restart and is not shared across consumer instances, so a
// rebalance moves partitions to instances that have no parking state.
// Treat parking as an ordering optimization, not a guarantee — consumers
// still need downstream dedup (inbox) and should tolerate rare reordering.
//
// SIZING: the window must cover the time until the escalated record is back
// in order — at least Tiers[0] (the first-tier delay) plus the retry
// consumer's redelivery lag (poll interval + processing). A window shorter
// than that un-parks the key before the escalated record is redelivered,
// re-introducing the reorder the parking was meant to prevent. For
// DefaultPolicy (tier-0 = 5s) a window of ~10s is a sensible floor.
//
// This option overrides Policy.KeyParkingWindow when both are set.
func WithKeyParking(window time.Duration) EscalatorOption {
	return func(e *Escalator) { e.parkWindow = window }
}

// NewEscalator constructs an Escalator backed by the given producer and policy.
// Key parking is enabled when policy.KeyParkingWindow > 0; the WithKeyParking
// option, when given, overrides the policy field.
func NewEscalator(p producer, policy Policy, opts ...EscalatorOption) *Escalator {
	e := &Escalator{producer: p, policy: policy, parkWindow: policy.KeyParkingWindow}
	for _, o := range opts {
		o(e)
	}
	if e.parkWindow > 0 {
		e.parked = make(map[parkKey]time.Time)
	}
	return e
}

// Escalate routes rec (which failed with cause) to its next destination and
// returns the destination topic. attempt is read from rec's retry headers
// (0 when absent, i.e. first failure on the base topic).
//
// Routing logic:
//   - If cause is a permanent apperr (apperr.IsPermanent) → destination is
//     DLTTopic(origTopic), skipping every remaining tier: no redelivery can
//     fix the payload. The DLT record keeps retry-last-error and adds the
//     x-error-code header with the apperr code.
//   - If attempt >= len(policy.Tiers) → destination is DLTTopic(origTopic).
//   - Otherwise → destination is TierTopic(origTopic, attempt) — the
//     zero-based tier INDEX names the topic; the tier delay only sets the
//     retry-due-at header.
//
// The record is published with all original headers preserved plus the four
// retry headers. The caller must commit the original record after Escalate
// returns nil. When key parking is enabled, a successful escalation parks
// the record's key for the parking window.
func (e *Escalator) Escalate(ctx context.Context, origTopic string, rec kafka.Record, cause error) (string, error) {
	dest, err := e.publishNext(ctx, origTopic, rec, cause)
	if err != nil {
		return "", err
	}
	e.park(origTopic, rec.Key)
	return dest, nil
}

// Divert routes rec straight into the retry pipeline WITHOUT a handler
// failure: it is used by WrapHandler when rec's key is currently parked, so
// the record queues up behind the previously-escalated one instead of being
// processed out of order. Attempt headers are handled exactly like Escalate
// (a base-topic record without headers goes to tier 0 with attempt=1; a
// record that already carries attempt headers continues from its tier).
// Divert does NOT extend the parking window — only real escalations park.
func (e *Escalator) Divert(ctx context.Context, origTopic string, rec kafka.Record) (string, error) {
	return e.publishNext(ctx, origTopic, rec, errKeyParked)
}

// errKeyParked is the synthetic cause recorded on diverted records.
var errKeyParked = errors.New("retry: key parked — diverted to preserve per-key order")

// publishNext builds and produces the next-destination record (shared by
// Escalate and Divert).
func (e *Escalator) publishNext(ctx context.Context, origTopic string, rec kafka.Record, cause error) (string, error) {
	attempt, _, _, ok := ParseRetryHeaders(rec)
	if !ok {
		attempt = 0
	}

	// Permanent errors skip every remaining tier: redelivery cannot fix the
	// payload, so the record goes straight to the DLT.
	toDLT := attempt >= len(e.policy.Tiers) || apperr.IsPermanent(cause)

	// Build the destination record with a copy of all headers.
	dest := TierTopic(origTopic, attempt)
	if toDLT {
		dest = DLTTopic(origTopic)
	}
	out := kafka.Record{
		Topic:   dest,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: copyHeaders(rec.Headers),
	}

	if toDLT {
		// Route to DLT (final tier exhausted or permanent error). Keep the
		// existing retry headers for forensics and add/overwrite last-error;
		// stamp the apperr code when the chain carries one. The origin topic
		// MUST be present: a base-topic record short-circuited by a permanent
		// error never traversed a tier, so it carries no retry-orig-topic —
		// without x-original-topic cmd/redrive cannot route it back and
		// aborts the whole redrive run at this record.
		out.Headers[HeaderLastError] = truncate(cause.Error(), 512)
		out.Headers[kafka.HeaderDLTOriginalTopic] = origTopic
		var ae *apperr.Error
		if errors.As(cause, &ae) {
			out.Headers[kafka.HeaderDLTErrorCode] = ae.Code
		}
	} else {
		due := time.Now().Add(e.policy.Tiers[attempt])
		SetRetryHeaders(&out, attempt+1, origTopic, due, cause)
	}

	if err := e.producer.Produce(ctx, out); err != nil {
		return "", fmt.Errorf("retry: escalate to %s: %w", dest, err)
	}

	return dest, nil
}

// park records that key on topic was escalated; no-op when parking disabled
// or the key is empty (keyless records have no per-key order to preserve).
func (e *Escalator) park(topic string, key []byte) {
	if e.parkWindow <= 0 || len(key) == 0 {
		return
	}
	now := time.Now()
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	e.parked[parkKey{topic: topic, key: string(key)}] = now.Add(e.parkWindow)
	e.parkOps++
	if e.parkOps >= parkSweepEvery {
		e.parkOps = 0
		for k, until := range e.parked {
			if now.After(until) {
				delete(e.parked, k)
			}
		}
	}
}

// KeyParked reports whether key on the given base topic is currently parked
// (escalated within the parking window). Expired entries are pruned on
// lookup. Always false when parking is disabled or key is empty.
func (e *Escalator) KeyParked(topic string, key []byte) bool {
	if e.parkWindow <= 0 || len(key) == 0 {
		return false
	}
	pk := parkKey{topic: topic, key: string(key)}
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	until, ok := e.parked[pk]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(e.parked, pk)
		return false
	}
	return true
}

// WrapHandler builds the base-topic consumer handler for tiered retry:
//
//  1. If the record's key is parked (recently escalated), the record is
//     DIVERTED straight to the first retry tier — the handler never sees it,
//     so its order relative to the escalated record is preserved.
//  2. Otherwise the handler runs up to policy.FastAttempts times in-process
//     (with a ctx-aware policy.FastBackoff sleep between attempts; values
//     <= 0 default to 100ms). A PERMANENT apperr (apperr.IsPermanent) stops
//     the loop after the first failure — the escalator then routes the
//     record straight to the DLT instead of a retry tier.
//  3. When all fast attempts fail the record is escalated to the next retry
//     tier. A nil return means the consumer may commit the offset; an
//     escalation/diversion produce failure is returned so the record is NOT
//     committed and will be redelivered (never dropped).
func WrapHandler(handler kafka.HandlerFunc, esc *Escalator, policy Policy) kafka.HandlerFunc {
	fastAttempts := policy.FastAttempts
	if fastAttempts <= 0 {
		fastAttempts = 1
	}
	fastBackoff := policy.FastBackoff
	if fastBackoff <= 0 {
		fastBackoff = 100 * time.Millisecond
	}
	return func(ctx context.Context, rec kafka.Record) error {
		// Key parked → divert without handling (ordering, see package doc).
		if esc.KeyParked(rec.Topic, rec.Key) {
			_, err := esc.Divert(ctx, rec.Topic, rec)
			return err
		}

		var lastErr error
		for attempt := 1; attempt <= fastAttempts; attempt++ {
			lastErr = handler(ctx, rec)
			if lastErr == nil {
				return nil
			}
			// Permanent → no point in further fast attempts; Escalate below
			// routes the record straight to the DLT.
			if apperr.IsPermanent(lastErr) {
				break
			}
			if attempt < fastAttempts {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(fastBackoff):
				}
			}
		}
		// All fast attempts exhausted — escalate to the next retry tier.
		if _, err := esc.Escalate(ctx, rec.Topic, rec, lastErr); err != nil {
			return err
		}
		// Successful escalation: commit; the retry consumer redelivers later.
		return nil
	}
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
