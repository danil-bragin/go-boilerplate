// Package retry provides tiered retry-topic redrive for Kafka consumers.
// Failed records are published to "<base>.retry.<delay>" topics with a
// due-time header and committed — the main partition is never blocked.
// A separate retry consumer (Task 2) redelivers records when their due
// time is reached; after the final tier the record goes to "<base>.DLT".
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
	Tiers []time.Duration

	// FastAttempts is the number of in-process handler attempts before the
	// first escalation to a retry topic. Values <= 0 are treated as 1.
	FastAttempts int
}

// DefaultPolicy returns the recommended three-tier policy: 5 s → 30 s → 5 m.
func DefaultPolicy() Policy {
	return Policy{
		Tiers:        []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute},
		FastAttempts: 1,
	}
}

// TierTopic returns the retry topic name for a given base topic and delay,
// e.g. TierTopic("orders.commands", 5*time.Second) == "orders.commands.retry.5s".
func TierTopic(base string, delay time.Duration) string {
	return base + ".retry." + delay.String()
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

// BaseTopic strips the ".retry.<dur>" suffix from a retry-topic name and
// returns the base topic. ok is false if the input does not match the pattern.
//
// Examples:
//
//	BaseTopic("orders.commands.retry.5s")   → ("orders.commands", true)
//	BaseTopic("orders.commands.retry.5m0s") → ("orders.commands", true)
//	BaseTopic("orders.commands")            → ("", false)
func BaseTopic(retryTopic string) (base string, ok bool) {
	// Walk backwards: find the second-to-last "." to locate ".retry.<dur>".
	// A valid retry topic is "<base>.retry.<dur>" where <dur> contains no ".".
	// We split on ".retry." which is unambiguous because duration strings
	// use only digits and time-unit letters (s, m, h, µ, n) — no dots.
	const marker = ".retry."
	idx := strings.LastIndex(retryTopic, marker)
	if idx < 0 {
		return "", false
	}
	// There must be at least one character after the marker (the duration).
	if idx+len(marker) >= len(retryTopic) {
		return "", false
	}
	// The base portion must be non-empty.
	if idx == 0 {
		return "", false
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
