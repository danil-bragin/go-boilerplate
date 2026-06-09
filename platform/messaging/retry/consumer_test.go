package retry_test

import (
	"testing"

	"go-boilerplate/platform/messaging/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// BaseTopic helper
// ---------------------------------------------------------------------------

func TestBaseTopic_ValidRetryTopics(t *testing.T) {
	tests := []struct {
		input    string
		wantBase string
	}{
		{"orders.commands.retry.0", "orders.commands"},
		{"orders.commands.retry.1", "orders.commands"},
		{"orders.commands.retry.2", "orders.commands"},
		{"payments.events.retry.10", "payments.events"},
		{"simple.retry.3", "simple"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, ok := retry.BaseTopic(tc.input)
			require.True(t, ok, "expected ok=true for %q", tc.input)
			assert.Equal(t, tc.wantBase, got)
		})
	}
}

func TestBaseTopic_NonRetryTopics(t *testing.T) {
	inputs := []string{
		"orders.commands",
		"orders.commands.DLT",
		"",
		".retry.",
		"base",
		// Legacy duration-suffixed names from the pre-index naming scheme are
		// intentionally NOT recognized anymore (see package doc migration note).
		"orders.commands.retry.5s",
		"orders.commands.retry.5m0s",
		// Non-numeric suffixes are not tier topics.
		"orders.commands.retry.x",
		".retry.0",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			_, ok := retry.BaseTopic(input)
			assert.False(t, ok, "expected ok=false for %q", input)
		})
	}
}

// TestBaseTopic_RoundTrip verifies BaseTopic(TierTopic(base, i)) == base
// for all tier indexes of the default policy.
func TestBaseTopic_RoundTrip(t *testing.T) {
	pol := retry.DefaultPolicy()
	bases := []string{"orders.commands", "payments.events", "foo.bar.baz"}

	for _, base := range bases {
		for i := range pol.Tiers {
			tierTopic := retry.TierTopic(base, i)
			got, ok := retry.BaseTopic(tierTopic)
			require.Truef(t, ok, "BaseTopic(%q) returned ok=false", tierTopic)
			assert.Equal(t, base, got, "BaseTopic(TierTopic(%q, %d))", base, i)
		}
	}
}

// TestBaseTopic_RoundTrip_ManyIndexes verifies round-trip for a broader range
// of tier indexes (multi-digit included).
func TestBaseTopic_RoundTrip_ManyIndexes(t *testing.T) {
	for _, i := range []int{0, 1, 2, 9, 10, 42} {
		topic := retry.TierTopic("base.topic", i)
		got, ok := retry.BaseTopic(topic)
		require.Truef(t, ok, "BaseTopic(%q) should succeed", topic)
		assert.Equal(t, "base.topic", got)
	}
}
