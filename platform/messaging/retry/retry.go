// Package retry provides tiered retry-topic redrive for Kafka consumers.
// Failed records are published to "<base>.retry.<idx>" tier topics (idx is
// the ZERO-BASED TIER INDEX: ".retry.0", ".retry.1", …) with a due-time
// header and committed — the main partition is never blocked. A separate
// retry consumer redelivers records when their due time is reached; after
// the final tier the record goes to "<base>.DLT".
//
// # ORDERING WARNING — read this before using tiered retry
//
// Tiered retry BREAKS PER-KEY ORDERING. When a record for key K is escalated
// to a retry tier, later records for K on the base topic keep flowing and are
// processed BEFORE the escalated record is redelivered. Consumers that apply
// events to per-key state (projections, state machines, balances) will see
// updates out of order. You have three options:
//
//  1. Make the consumer reorder-safe (idempotent, last-write-wins by version,
//     or upsert semantics) — the recommended default for projections.
//  2. Enable KEY PARKING (Policy.KeyParkingWindow / WithKeyParking): once a
//     key is escalated, subsequent records with the same key on the same base
//     topic are diverted straight to the first retry tier instead of being
//     handled, so the retry pipeline preserves their relative order. Parking
//     is in-memory and BEST-EFFORT: it is lost on restart/rebalance, and each
//     consumer instance only parks keys whose escalation it performed.
//  3. Do not use tiered retry for that consumer (use the blocking
//     in-process retry of kafka.WithRetry, which never reorders).
//
// If none of the above is applied, tiered retry silently reorders per-key
// records under failure — which is exactly when correctness matters most.
//
// # Migration note: tier topic naming changed
//
// Tier topics were previously named after the tier DELAY ("<base>.retry.5s",
// "<base>.retry.5m0s"), which meant tuning the policy stranded in-flight
// records on orphaned topics. Names are now INDEX-based ("<base>.retry.0",
// "<base>.retry.1", …) and the delay is carried exclusively in the
// retry-due-at header, so delays can be tuned without renaming topics.
// When upgrading a deployment that still has records on old duration-named
// topics: either keep one old-build consumer running until those topics
// drain, or manually redrive their records onto the base topic.
package retry

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go-boilerplate/platform/messaging/kafka"
)

// Header name constants written onto retry-topic records.
const (
	// HeaderAttempt is the number of escalations already performed.
	// "1" means the record has been escalated once (is now in tier-0 topic).
	HeaderAttempt = "retry-attempt"

	// HeaderOrigTopic is the original source topic before any escalation.
	HeaderOrigTopic = "retry-orig-topic"

	// HeaderDueAt is the unix milliseconds (as a decimal string) at which
	// the retry consumer may redeliver this record.
	HeaderDueAt = "retry-due-at"

	// HeaderLastError is the cause.Error() string, truncated to 512 bytes.
	HeaderLastError = "retry-last-error"
)

// Policy configures tiered retry-topic redrive.
type Policy struct {
	// Tiers lists the escalation delays in order.
	// A record failing in tier i is escalated to tier i+1 after Tiers[i].
	// After the last tier the record is sent to the DLT.
	//
	// The delay of a tier is carried in the retry-due-at header of each
	// escalated record, NOT in the topic name — tier topics are index-named
	// ("<base>.retry.0", …), so Tiers can be tuned without renaming topics.
	// Records already in flight keep their original due time.
	Tiers []time.Duration

	// FastAttempts is the number of in-process handler attempts before the
	// first escalation to a retry topic. Values <= 0 are treated as 1.
	FastAttempts int

	// KeyParkingWindow, when > 0, enables opt-in key parking (see the
	// package-level ORDERING WARNING): after a key is escalated, records with
	// the same key arriving on the same base topic within the window are
	// diverted straight to the first retry tier instead of being handled,
	// preserving their order relative to the escalated record. In-memory
	// best-effort — parking is lost on restart or rebalance.
	//
	// SIZING: choose window ≥ Tiers[0] + the retry consumer's redelivery lag,
	// otherwise the key un-parks before the escalated record returns and the
	// reorder happens anyway. For DefaultPolicy (tier-0 = 5s) ~10s is a
	// sensible floor. NewEscalator honors this field; the WithKeyParking
	// option overrides it.
	KeyParkingWindow time.Duration
}

// DefaultPolicy returns the recommended three-tier policy: 5 s → 30 s → 5 m.
func DefaultPolicy() Policy {
	return Policy{
		Tiers:        []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute},
		FastAttempts: 1,
	}
}

// TierTopic returns the retry topic name for a given base topic and
// ZERO-BASED tier index, e.g. TierTopic("orders.commands", 0) ==
// "orders.commands.retry.0". The tier's delay lives in the retry-due-at
// header of each record, never in the topic name, so retry policies can be
// tuned without stranding in-flight records (see package migration note).
func TierTopic(base string, idx int) string {
	return base + ".retry." + strconv.Itoa(idx)
}

// DLTTopic returns the dead-letter topic name for base, matching the
// convention in platform/messaging/kafka/dlq.go: base + ".DLT".
func DLTTopic(base string) string {
	return base + ".DLT"
}

// SetRetryHeaders writes the four retry headers onto rec.Headers.
// It overwrites any existing values for those keys.
// The map is created if nil (callers should initialise it themselves, but
// this guard prevents a nil-map panic for convenience).
func SetRetryHeaders(rec *kafka.Record, attempt int, origTopic string, due time.Time, cause error) {
	if rec.Headers == nil {
		rec.Headers = make(map[string]string)
	}
	rec.Headers[HeaderAttempt] = strconv.Itoa(attempt)
	rec.Headers[HeaderOrigTopic] = origTopic
	rec.Headers[HeaderDueAt] = strconv.FormatInt(due.UnixMilli(), 10)

	errStr := cause.Error()
	if utf8.RuneCountInString(errStr) > 512 {
		// Truncate at the 512-byte boundary without splitting a multi-byte rune.
		b := []byte(errStr)
		if len(b) > 512 {
			b = b[:512]
			// Walk back to a valid UTF-8 boundary.
			for !utf8.Valid(b) {
				b = b[:len(b)-1]
			}
		}
		errStr = string(b)
	}
	rec.Headers[HeaderLastError] = errStr
}

// BaseTopic strips the ".retry.<idx>" suffix from a retry-topic name and
// returns the base topic. ok is false if the input does not match the
// pattern (the suffix must be a decimal tier index).
//
// Examples:
//
//	BaseTopic("orders.commands.retry.0")  → ("orders.commands", true)
//	BaseTopic("orders.commands.retry.12") → ("orders.commands", true)
//	BaseTopic("orders.commands.retry.5s") → ("", false)  // legacy duration name
//	BaseTopic("orders.commands")          → ("", false)
func BaseTopic(retryTopic string) (base string, ok bool) {
	// A valid retry topic is "<base>.retry.<idx>" where <idx> is a decimal
	// integer (zero-based tier index). Splitting on the last ".retry." is
	// unambiguous because the index contains no dots.
	const marker = ".retry."
	idx := strings.LastIndex(retryTopic, marker)
	if idx <= 0 { // marker absent, or empty base portion
		return "", false
	}
	suffix := retryTopic[idx+len(marker):]
	if suffix == "" {
		return "", false
	}
	// The suffix must be all digits — legacy duration-named topics ("5s",
	// "5m0s") are intentionally rejected (see package migration note).
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	return retryTopic[:idx], true
}

// ParseRetryHeaders reads the retry headers from rec.
// ok is false if any of attempt, origTopic, or dueAt are absent or malformed.
func ParseRetryHeaders(rec kafka.Record) (attempt int, origTopic string, due time.Time, ok bool) {
	if rec.Headers == nil {
		return 0, "", time.Time{}, false
	}

	attemptStr, hasAttempt := rec.Headers[HeaderAttempt]
	origTopic, hasOrig := rec.Headers[HeaderOrigTopic]
	dueAtStr, hasDue := rec.Headers[HeaderDueAt]

	if !hasAttempt || !hasOrig || !hasDue {
		return 0, "", time.Time{}, false
	}

	a, err := strconv.Atoi(attemptStr)
	if err != nil {
		return 0, "", time.Time{}, false
	}

	ms, err := strconv.ParseInt(dueAtStr, 10, 64)
	if err != nil {
		return 0, "", time.Time{}, false
	}

	return a, origTopic, time.UnixMilli(ms), true
}
