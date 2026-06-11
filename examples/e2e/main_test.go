package e2e

import (
	"testing"

	"go-boilerplate/platform/testkit/goleakopts"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine hygiene across the e2e suite: every test wires
// all four services in-process, so a leaked consumer/relay/SSE goroutine from
// one test would bleed into the next and flake the whole choreography. The
// goleakopts set ignores the testcontainers reaper and franz-go client loops
// (process-lifetime singletons, not leaks in code under test).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakopts.Default()...)
}
