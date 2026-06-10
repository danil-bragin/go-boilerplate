package clock_test

import (
	"testing"
	"testing/synctest"
	"time"

	"go-boilerplate/platform/clock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: System satisfies Clock.
var _ clock.Clock = clock.System{}

func TestSystem_NowReturnsUTC(t *testing.T) {
	now := clock.System{}.Now()
	assert.Equal(t, time.UTC, now.Location(), "Clock implementations MUST return UTC")
}

// TestSystem_InSynctestBubble demonstrates the intended testing pattern for
// code that takes a Clock: inside a synctest bubble time is fake and
// deterministic, so business logic reading clock.System{}.Now() can be
// driven through time.Sleep without real waiting — no fake-clock
// implementation needed.
func TestSystem_InSynctestBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := clock.System{}
		start := c.Now()
		require.Equal(t, time.UTC, start.Location(), "UTC holds for fake time too")

		time.Sleep(5 * time.Minute) // instant: fake time inside the bubble

		assert.Equal(t, 5*time.Minute, c.Now().Sub(start))
	})
}
