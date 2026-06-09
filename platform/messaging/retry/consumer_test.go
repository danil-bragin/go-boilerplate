package retry_test

import (
	"testing"
	"time"

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
		{"orders.commands.retry.5s", "orders.commands"},
		{"orders.commands.retry.30s", "orders.commands"},
		{"orders.commands.retry.5m0s", "orders.commands"},
		{"payments.events.retry.1m0s", "payments.events"},
		{"simple.retry.10s", "simple"},
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
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			_, ok := retry.BaseTopic(input)
			assert.False(t, ok, "expected ok=false for %q", input)
		})
	}
}

// TestBaseTopic_RoundTrip verifies BaseTopic(TierTopic(base, d)) == base
// for all tiers of the default policy.
func TestBaseTopic_RoundTrip(t *testing.T) {
	pol := retry.DefaultPolicy()
	bases := []string{"orders.commands", "payments.events", "foo.bar.baz"}

	for _, base := range bases {
		for _, d := range pol.Tiers {
			tierTopic := retry.TierTopic(base, d)
			got, ok := retry.BaseTopic(tierTopic)
			require.Truef(t, ok, "BaseTopic(%q) returned ok=false", tierTopic)
			assert.Equal(t, base, got, "BaseTopic(TierTopic(%q, %v))", base, d)
		}
	}
}

// TestBaseTopic_RoundTrip_CustomDelays verifies round-trip with a broader set
// of durations that Duration.String() may produce.
func TestBaseTopic_RoundTrip_CustomDelays(t *testing.T) {
	delays := []time.Duration{
		5 * time.Second,
		30 * time.Second,
		5 * time.Minute,
		1 * time.Hour,
		500 * time.Millisecond,
	}
	for _, d := range delays {
		topic := retry.TierTopic("base.topic", d)
		got, ok := retry.BaseTopic(topic)
		require.Truef(t, ok, "BaseTopic(%q) should succeed", topic)
		assert.Equal(t, "base.topic", got)
	}
}
