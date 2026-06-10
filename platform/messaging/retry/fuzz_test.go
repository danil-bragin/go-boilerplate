package retry_test

import (
	"strconv"
	"testing"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/retry"
)

// FuzzParseRetryHeaders hammers the retry-header parser with arbitrary header
// values and presence combinations. Retry headers travel on Kafka records and
// can be written by external tools (redrive, manual rpk produce), so the
// parser must treat them as untrusted input. Invariants:
//
//  1. Never panics.
//  2. ok=true requires all three mandatory headers present AND parseable;
//     the returned values must agree with an independent re-parse.
//  3. Any missing or malformed mandatory header → ok=false with zero values.
func FuzzParseRetryHeaders(f *testing.F) {
	// Seeds from retry_test.go.
	f.Add("1", "orders.commands", "1234567890000", true, true, true)
	f.Add("", "orders.commands", "1234567890000", false, true, true)  // missing attempt
	f.Add("1", "", "1234567890000", true, false, true)                // missing orig topic
	f.Add("1", "orders.commands", "", true, true, false)              // missing due-at
	f.Add("1", "orders.commands", "not-a-number", true, true, true)   // malformed due-at
	f.Add("not-a-number", "orders.commands", "123", true, true, true) // malformed attempt
	f.Add("-1", "t", "-9223372036854775808", true, true, true)
	f.Add("9999999999999999999999", "t", "99999999999999999999", true, true, true)

	f.Fuzz(func(t *testing.T, attempt, origTopic, dueAt string, hasAttempt, hasOrig, hasDue bool) {
		headers := map[string]string{}
		if hasAttempt {
			headers[retry.HeaderAttempt] = attempt
		}
		if hasOrig {
			headers[retry.HeaderOrigTopic] = origTopic
		}
		if hasDue {
			headers[retry.HeaderDueAt] = dueAt
		}
		rec := kafka.Record{Topic: "orders.commands", Headers: headers}

		gotAttempt, gotOrig, gotDue, ok := retry.ParseRetryHeaders(rec)

		if !ok {
			if gotAttempt != 0 || gotOrig != "" || !gotDue.IsZero() {
				t.Fatalf("ok=false must return zero values, got (%d, %q, %v)", gotAttempt, gotOrig, gotDue)
			}
			return
		}

		// ok=true: all mandatory headers were present and parseable.
		if !hasAttempt || !hasOrig || !hasDue {
			t.Fatalf("ok=true with a missing mandatory header (attempt=%v orig=%v due=%v)", hasAttempt, hasOrig, hasDue)
		}
		wantAttempt, err := strconv.Atoi(attempt)
		if err != nil {
			t.Fatalf("ok=true with unparseable attempt %q", attempt)
		}
		wantMillis, err := strconv.ParseInt(dueAt, 10, 64)
		if err != nil {
			t.Fatalf("ok=true with unparseable due-at %q", dueAt)
		}
		if gotAttempt != wantAttempt || gotOrig != origTopic || gotDue.UnixMilli() != wantMillis {
			t.Fatalf("parsed values disagree: got (%d, %q, %d) want (%d, %q, %d)",
				gotAttempt, gotOrig, gotDue.UnixMilli(), wantAttempt, origTopic, wantMillis)
		}
	})
}
