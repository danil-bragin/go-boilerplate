package kafka_test

import (
	"os"
	"testing"

	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
)

// TestMain terminates the package-shared Redpanda container (started lazily
// by the first kafkatest.Shared call) after all tests have run. Sharing one
// broker across the package cuts the per-test ~5s container startup; tests
// stay isolated by using uniqueName for topics and consumer groups.
func TestMain(m *testing.M) {
	code := m.Run()
	kafkatest.TerminateShared()
	os.Exit(code)
}

// uniqueName returns prefix + "-" + 8 random hex chars. Topics and group IDs
// on the shared broker MUST be unique per test to avoid cross-test leakage
// (leftover records, committed offsets).
func uniqueName(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}
