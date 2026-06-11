package traffic

import (
	"testing"

	"go-boilerplate/platform/testkit/goleakopts"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine hygiene for the whole package: the traffic
// harness spawns worker goroutines per phase, and a leaked worker would
// silently skew every seeded-run repro. Same goleakopts set as the rest of
// the repo's TestMains.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakopts.Default()...)
}
