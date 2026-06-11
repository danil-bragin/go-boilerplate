package traffic

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopMix returns a two-scenario mix whose Run functions do nothing.
func noopMix() []Scenario {
	return []Scenario{
		{Name: "a", Weight: 70, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
		{Name: "b", Weight: 30, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
	}
}

func TestBuildSchedule_DeterministicForSameSeed(t *testing.T) {
	phases := []Phase{{Rate: 500, Duration: time.Second}, {Rate: 100, Duration: 500 * time.Millisecond}}
	mix := noopMix()

	s1 := buildSchedule(42, phases, mix)
	s2 := buildSchedule(42, phases, mix)
	require.Equal(t, s1, s2, "same seed must produce an identical op schedule")

	s3 := buildSchedule(43, phases, mix)
	require.NotEqual(t, s1, s3, "a different seed must produce a different schedule")
}

func TestBuildSchedule_PoissonMeanWithinTolerance(t *testing.T) {
	// rate 1000/s over 2s virtual time → ~2000 ops. The seed is fixed, so the
	// count is deterministic; the tolerance only documents the statistical
	// expectation (sd ≈ sqrt(2000) ≈ 45).
	ops := buildSchedule(42, []Phase{{Rate: 1000, Duration: 2 * time.Second}}, noopMix())
	n := len(ops)
	assert.InDelta(t, 2000, n, 200, "op count should be within ~10%% of rate×duration (got %d)", n)
}

func TestBuildSchedule_WeightedMixDistribution(t *testing.T) {
	mix := []Scenario{
		{Name: "x", Weight: 70, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
		{Name: "y", Weight: 20, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
		{Name: "z", Weight: 10, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
	}
	ops := buildSchedule(7, []Phase{{Rate: 5000, Duration: time.Second}}, mix)
	require.NotEmpty(t, ops)

	counts := map[int]int{}
	for _, o := range ops {
		counts[o.scenario]++
	}
	total := float64(len(ops))
	assert.InDelta(t, 0.70, float64(counts[0])/total, 0.03, "scenario x share")
	assert.InDelta(t, 0.20, float64(counts[1])/total, 0.03, "scenario y share")
	assert.InDelta(t, 0.10, float64(counts[2])/total, 0.03, "scenario z share")
}

func TestBuildSchedule_PhasesTiming(t *testing.T) {
	phases := []Phase{
		{Rate: 100, Duration: time.Second},
		{Rate: 1000, Duration: time.Second},
	}
	ops := buildSchedule(11, phases, noopMix())
	require.NotEmpty(t, ops)

	var inFirst, inSecond int
	last := time.Duration(-1)
	for _, o := range ops {
		require.Greater(t, o.offset, time.Duration(0))
		require.LessOrEqual(t, o.offset, 2*time.Second, "no op may be scheduled past the total duration")
		require.Greater(t, o.offset, last, "arrival offsets must be strictly increasing")
		last = o.offset
		if o.offset <= time.Second {
			inFirst++
		} else {
			inSecond++
		}
	}
	// Fixed seed → deterministic counts; the bounds document the expectation.
	assert.InDelta(t, 100, inFirst, 35, "phase 1 (100rps × 1s) op count")
	assert.InDelta(t, 1000, inSecond, 110, "phase 2 (1000rps × 1s) op count")
}

func TestGenerator_RunIsReproducibleFromSeed(t *testing.T) {
	// Same seed → the same multiset of (scenario, first rng draw) generation
	// decisions, regardless of goroutine scheduling. Wall-clock interleaving
	// is explicitly NOT reproducible (see package doc), so we compare sorted.
	run := func() []string {
		var mu sync.Mutex
		var got []string
		record := func(name string) func(context.Context, *rand.Rand, *Ledger) error {
			return func(_ context.Context, rng *rand.Rand, _ *Ledger) error {
				mu.Lock()
				got = append(got, fmt.Sprintf("%s-%016x", name, rng.Uint64()))
				mu.Unlock()
				return nil
			}
		}
		mix := []Scenario{
			{Name: "a", Weight: 60, Run: record("a")},
			{Name: "b", Weight: 40, Run: record("b")},
		}
		g, err := NewGenerator(Config{Seed: 42, Workers: 8, Phases: []Phase{{Rate: 800, Duration: 150 * time.Millisecond}}}, mix)
		require.NoError(t, err)
		res, err := g.Run(context.Background(), NewLedger())
		require.NoError(t, err)
		require.EqualValues(t, 42, res.Seed)
		sort.Strings(got)
		return got
	}

	first := run()
	second := run()
	require.NotEmpty(t, first)
	require.Equal(t, first, second)
}

func TestGenerator_ZeroSeedIsDerivedAndReported(t *testing.T) {
	g, err := NewGenerator(Config{Phases: []Phase{{Rate: 200, Duration: 50 * time.Millisecond}}}, noopMix())
	require.NoError(t, err)
	res, err := g.Run(context.Background(), NewLedger())
	require.NoError(t, err)
	assert.NotZero(t, res.Seed, "seed 0 must be replaced by a derived seed and reported")
}

func TestGenerator_ResultCountsAndErrorCodes(t *testing.T) {
	mix := []Scenario{
		{Name: "ok", Weight: 50, Run: func(context.Context, *rand.Rand, *Ledger) error { return nil }},
		{Name: "boom", Weight: 50, Run: func(context.Context, *rand.Rand, *Ledger) error {
			return CodedError("BOOM", errors.New("kaput"))
		}},
	}
	g, err := NewGenerator(Config{Seed: 1, Workers: 4, Phases: []Phase{{Rate: 600, Duration: 100 * time.Millisecond}}}, mix)
	require.NoError(t, err)
	res, err := g.Run(context.Background(), NewLedger())
	require.NoError(t, err)

	okStats := res.Scenario("ok")
	boomStats := res.Scenario("boom")
	require.NotZero(t, okStats.Started)
	require.NotZero(t, boomStats.Started)
	assert.Equal(t, okStats.Started, okStats.OK)
	assert.Zero(t, okStats.Failed)
	assert.Equal(t, boomStats.Started, boomStats.Failed)
	assert.Equal(t, boomStats.Failed, boomStats.ErrorsByCode["BOOM"])
	assert.Equal(t, boomStats.Failed, res.TotalFailed())

	// Latency: every completed op contributes a sample.
	q, ok := res.Quantile("ok", 0.5)
	require.True(t, ok)
	assert.GreaterOrEqual(t, q, time.Duration(0))

	// Summary table mentions every scenario.
	s := res.String()
	assert.Contains(t, s, "ok")
	assert.Contains(t, s, "boom")
	assert.Contains(t, s, "seed=1")
}

func TestGenerator_ValidatesConfig(t *testing.T) {
	_, err := NewGenerator(Config{Phases: []Phase{{Rate: 10, Duration: time.Second}}}, nil)
	require.Error(t, err, "empty mix")
	_, err = NewGenerator(Config{}, noopMix())
	require.Error(t, err, "no phases")
	_, err = NewGenerator(Config{Phases: []Phase{{Rate: 0, Duration: time.Second}}}, noopMix())
	require.Error(t, err, "zero rate")
	_, err = NewGenerator(Config{Phases: []Phase{{Rate: 10, Duration: 0}}}, noopMix())
	require.Error(t, err, "zero duration")
	mix := noopMix()
	mix[0].Weight = 0
	_, err = NewGenerator(Config{Phases: []Phase{{Rate: 10, Duration: time.Second}}}, mix)
	require.Error(t, err, "non-positive weight")
}

func TestGenerator_RunHonorsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g, err := NewGenerator(Config{Seed: 1, Phases: []Phase{{Rate: 10, Duration: 10 * time.Second}}}, noopMix())
	require.NoError(t, err)

	start := time.Now()
	_, err = g.Run(ctx, NewLedger())
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 2*time.Second, "cancelled Run must return promptly")
}
