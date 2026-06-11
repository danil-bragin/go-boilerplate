package traffic

import (
	"testing"

	"go-boilerplate/platform/testkit/goleakopts"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine hygiene for the gateway traffic pack: scenarios
// open SSE streams and HTTP connections against httptest servers, and a
// leaked stream goroutine here would mask the very races the pack exists to
// regression-protect.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakopts.Default()...)
}
