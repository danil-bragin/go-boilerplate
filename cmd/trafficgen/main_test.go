package main

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kit "go-boilerplate/platform/testkit/traffic"
)

func TestParsePhases(t *testing.T) {
	phases, err := parsePhases("10rps:5s,40rps:20s")
	require.NoError(t, err)
	require.Equal(t, []kit.Phase{
		{Rate: 10, Duration: 5 * time.Second},
		{Rate: 40, Duration: 20 * time.Second},
	}, phases)

	// The "rps" suffix is optional; fractional rates are allowed.
	phases, err = parsePhases("0.5:1m")
	require.NoError(t, err)
	require.Equal(t, []kit.Phase{{Rate: 0.5, Duration: time.Minute}}, phases)

	for _, bad := range []string{"", "10rps", "10rps:xx", "abc:5s", "-1rps:5s", "10rps:5s,"} {
		_, err := parsePhases(bad)
		require.Error(t, err, "input %q", bad)
	}
}

func TestParseMix(t *testing.T) {
	weights, err := parseMix("happy=70,decline=10,sse=0")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"happy": 70, "decline": 10, "sse": 0}, weights)

	for _, bad := range []string{"happy", "happy=x", "happy=-1", "=5"} {
		_, err := parseMix(bad)
		require.Error(t, err, "input %q", bad)
	}
}

func TestApplyMix(t *testing.T) {
	mk := func(names ...string) []kit.Scenario {
		out := make([]kit.Scenario, 0, len(names))
		for _, n := range names {
			out = append(out, kit.Scenario{
				Name: n, Weight: 1,
				Run: func(context.Context, *rand.Rand, *kit.Ledger) error { return nil },
			})
		}
		return out
	}

	t.Run("reweights and drops zero-weight scenarios", func(t *testing.T) {
		got, err := applyMix(mk("a", "b", "c"), map[string]int{"a": 9, "b": 0})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "a", got[0].Name)
		assert.Equal(t, 9, got[0].Weight)
		assert.Equal(t, "c", got[1].Name)
		assert.Equal(t, 1, got[1].Weight, "unmentioned scenarios keep their pack weight")
	})

	t.Run("unknown scenario name is an error", func(t *testing.T) {
		_, err := applyMix(mk("a"), map[string]int{"nope": 5})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nope")
	})

	t.Run("empty mix keeps the pack as-is", func(t *testing.T) {
		got, err := applyMix(mk("a", "b"), nil)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})
}
