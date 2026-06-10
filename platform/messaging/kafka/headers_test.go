package kafka_test

import (
	"testing"

	"go-boilerplate/platform/messaging/kafka"
)

// TestHeaderConstants_WireValues pins the exact header strings. These are
// wire values — records in flight and parked in DLTs carry them — so any
// change here is a breaking protocol change, not a rename.
func TestHeaderConstants_WireValues(t *testing.T) {
	want := map[string]string{
		"HeaderMessageID":        "message-id",
		"HeaderEventType":        "event-type",
		"HeaderDLTError":         "x-error",
		"HeaderDLTAttempts":      "x-attempts",
		"HeaderDLTOriginalTopic": "x-original-topic",
	}
	got := map[string]string{
		"HeaderMessageID":        kafka.HeaderMessageID,
		"HeaderEventType":        kafka.HeaderEventType,
		"HeaderDLTError":         kafka.HeaderDLTError,
		"HeaderDLTAttempts":      kafka.HeaderDLTAttempts,
		"HeaderDLTOriginalTopic": kafka.HeaderDLTOriginalTopic,
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("kafka.%s = %q, want wire value %q", name, got[name], w)
		}
	}
}
