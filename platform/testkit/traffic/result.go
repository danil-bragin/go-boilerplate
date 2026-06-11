package traffic

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxReservoir bounds the per-scenario latency sample reservoir. Beyond it,
// uniform reservoir sampling (Algorithm R) keeps an unbiased sample.
const maxReservoir = 100_000

// codedError tags a scenario error with a stable code for Result's
// errors-by-code breakdown.
type codedError struct {
	code string
	err  error
}

// CodedError wraps err with a stable code (e.g. an HTTP status bucket or a
// problem+json code) so [Result] can aggregate failures meaningfully.
func CodedError(code string, err error) error {
	return &codedError{code: code, err: err}
}

func (e *codedError) Error() string { return e.code + ": " + e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }

// errorCode extracts the aggregation code from a scenario error.
func errorCode(err error) string {
	var ce *codedError
	if errors.As(err, &ce) {
		return ce.code
	}
	return "UNCODED"
}

// ScenarioStats aggregates one scenario's outcomes.
type ScenarioStats struct {
	Started      int
	OK           int
	Failed       int
	ErrorsByCode map[string]int

	samples []time.Duration // latency reservoir (≤ maxReservoir)
}

// Result aggregates a [Generator.Run]: per-scenario counts, error codes,
// and bounded latency reservoirs. Safe for concurrent record during the run;
// read it only after Run returns.
type Result struct {
	// Seed is the resolved master seed (never 0) — log it: re-running with
	// the same seed replays the exact generation sequence.
	Seed int64
	// Elapsed is the wall-clock duration of the run.
	Elapsed time.Duration

	mu        sync.Mutex
	rng       *rand.Rand // reservoir sampling decisions
	scenarios map[string]*ScenarioStats
}

func newResult(seed int64) *Result {
	return &Result{
		Seed: seed,
		//nolint:gosec // G404/G115: reservoir sampling for test metrics, not a security context
		rng:       rand.New(rand.NewPCG(uint64(seed), 0xda942042e4dd58b5)),
		scenarios: make(map[string]*ScenarioStats),
	}
}

// record stores one op completion. err == nil counts as OK.
func (r *Result) record(scenario string, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.scenarios[scenario]
	if st == nil {
		st = &ScenarioStats{ErrorsByCode: make(map[string]int)}
		r.scenarios[scenario] = st
	}
	st.Started++
	if err != nil {
		st.Failed++
		st.ErrorsByCode[errorCode(err)]++
	} else {
		st.OK++
	}
	// Latency reservoir: plain append until full, then Algorithm R.
	if len(st.samples) < maxReservoir {
		st.samples = append(st.samples, latency)
	} else if i := r.rng.IntN(st.Started); i < maxReservoir {
		st.samples[i] = latency
	}
}

// Scenario returns a copy of the named scenario's stats (zero value when the
// scenario never ran).
func (r *Result) Scenario(name string) ScenarioStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.scenarios[name]
	if st == nil {
		return ScenarioStats{}
	}
	cp := *st
	cp.ErrorsByCode = make(map[string]int, len(st.ErrorsByCode))
	for k, v := range st.ErrorsByCode {
		cp.ErrorsByCode[k] = v
	}
	cp.samples = slices.Clone(st.samples)
	return cp
}

// TotalFailed sums failures across all scenarios.
func (r *Result) TotalFailed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, st := range r.scenarios {
		n += st.Failed
	}
	return n
}

// Quantile returns the p-quantile (0 < p ≤ 1) of the scenario's latency
// reservoir via a sorted copy. ok is false when the scenario has no samples.
func (r *Result) Quantile(scenario string, p float64) (d time.Duration, ok bool) {
	r.mu.Lock()
	st := r.scenarios[scenario]
	var samples []time.Duration
	if st != nil {
		samples = slices.Clone(st.samples)
	}
	r.mu.Unlock()
	if len(samples) == 0 {
		return 0, false
	}
	slices.Sort(samples)
	idx := int(p*float64(len(samples))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx], true
}

// String renders a per-scenario summary table.
func (r *Result) String() string {
	r.mu.Lock()
	names := make([]string, 0, len(r.scenarios))
	for name := range r.scenarios {
		names = append(names, name)
	}
	r.mu.Unlock()
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "traffic run: seed=%d elapsed=%s\n", r.Seed, r.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "%-12s %8s %8s %8s %10s %10s %10s  %s\n",
		"scenario", "started", "ok", "failed", "p50", "p95", "p99", "errors")
	for _, name := range names {
		st := r.Scenario(name)
		p50, _ := r.Quantile(name, 0.50)
		p95, _ := r.Quantile(name, 0.95)
		p99, _ := r.Quantile(name, 0.99)
		codes := make([]string, 0, len(st.ErrorsByCode))
		for code, n := range st.ErrorsByCode {
			codes = append(codes, fmt.Sprintf("%s=%d", code, n))
		}
		sort.Strings(codes)
		fmt.Fprintf(&b, "%-12s %8d %8d %8d %10s %10s %10s  %s\n",
			name, st.Started, st.OK, st.Failed,
			p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond),
			strings.Join(codes, " "))
	}
	return b.String()
}
