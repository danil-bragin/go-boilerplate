package featureflags_test

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/featureflags"
)

// flagMap builds the InMemoryFlag map used by most tests.
// Bool flags: "new-checkout"=true, "off-flag"=false.
// String flag: "greeting"="hello".
// Int flag: "page-size"=25.
func testFlagMap() map[string]memprovider.InMemoryFlag {
	return map[string]memprovider.InMemoryFlag{
		"new-checkout": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants:       map[string]any{"on": true, "off": false},
		},
		"off-flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "off",
			Variants:       map[string]any{"on": true, "off": false},
		},
		"greeting": {
			State:          memprovider.Enabled,
			DefaultVariant: "default",
			Variants:       map[string]any{"default": "hello"},
		},
		"page-size": {
			State:          memprovider.Enabled,
			DefaultVariant: "normal",
			Variants:       map[string]any{"normal": int64(25)},
		},
	}
}

// TestFlags_BoolReturnsConfiguredValue verifies that in-memory bool flags return
// their configured variant values.
func TestFlags_BoolReturnsConfiguredValue(t *testing.T) {
	t.Parallel()

	flags, err := featureflags.NewInMemory(t.Name(), testFlagMap())
	require.NoError(t, err)

	ctx := context.Background()

	// "new-checkout" → DefaultVariant "on" → true
	require.True(t, flags.Bool(ctx, "new-checkout", false), "new-checkout should be true")

	// "off-flag" → DefaultVariant "off" → false
	require.False(t, flags.Bool(ctx, "off-flag", true), "off-flag should be false")
}

// TestFlags_DefaultOnUnknownKey verifies that the supplied default is returned
// when the flag key does not exist in the provider.
func TestFlags_DefaultOnUnknownKey(t *testing.T) {
	t.Parallel()

	flags, err := featureflags.NewInMemory(t.Name(), testFlagMap())
	require.NoError(t, err)

	ctx := context.Background()

	// "missing" is not in the map → default (true) should be returned.
	require.True(t, flags.Bool(ctx, "missing", true))

	// Ensure false default also works.
	require.False(t, flags.Bool(ctx, "missing", false))
}

// TestFlags_StringAndInt verifies string and int flag evaluation plus unknown-key
// defaults.
func TestFlags_StringAndInt(t *testing.T) {
	t.Parallel()

	flags, err := featureflags.NewInMemory(t.Name(), testFlagMap())
	require.NoError(t, err)

	ctx := context.Background()

	// Configured string flag.
	require.Equal(t, "hello", flags.String(ctx, "greeting", "fallback"))

	// Unknown string flag → default.
	require.Equal(t, "fallback", flags.String(ctx, "unknown-string", "fallback"))

	// Configured int flag.
	require.Equal(t, int64(25), flags.Int(ctx, "page-size", 0))

	// Unknown int flag → default.
	require.Equal(t, int64(99), flags.Int(ctx, "unknown-int", 99))
}
