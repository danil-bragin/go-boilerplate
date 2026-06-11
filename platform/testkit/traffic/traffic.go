// Package traffic is a seeded, scenario-based traffic generator for
// correctness-under-load testing. It is target-agnostic: scenarios are
// closures that drive any system (an HTTP API, a queue, a library) and
// record invariant expectations in a [Ledger], which is later checked
// against the system's observable state via [Probes].
//
// # Model
//
// A [Generator] runs a weighted mix of [Scenario]s through a bounded worker
// pool. Arrivals are Poisson: inter-arrival gaps are exponential draws with
// the phase's mean rate, so bursts and lulls occur naturally. [Phase]s chain
// rate changes (ramp → plateau → spike) into one run.
//
// # Determinism boundary
//
// Every GENERATION decision — how many ops a phase emits, each op's arrival
// offset, which scenario it runs, and every payload byte a scenario derives
// from its rng — is a pure function of the seed: the full op schedule
// (including a per-op PCG seed) is materialized up front from a master
// math/rand/v2 PCG stream, and each op's rng is seeded from the schedule,
// not from the worker that happens to execute it. Re-running with the same
// seed therefore replays the exact same operation sequence and payloads.
//
// What is NOT reproducible is wall-clock interleaving: which worker executes
// an op, how in-flight ops overlap, and every latency measurement depend on
// goroutine scheduling and the target's timing. Invariants asserted through
// the [Ledger] must therefore hold under ANY interleaving — that is the
// point of the tool.
//
// A Config.Seed of 0 derives a seed from the wall clock; the resolved seed
// is always reported in [Result].Seed — log it, so a red run can be replayed.
package traffic

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Scenario is one weighted operation type in the mix. Run drives the target
// once: it derives all payload decisions from rng (so generation is
// reproducible from the seed) and records invariant expectations in the
// ledger. The returned error marks the op failed in [Result] (keyed by
// [CodedError] code when wrapped); it does NOT abort the run.
type Scenario struct {
	Name   string
	Weight int
	Run    func(ctx context.Context, rng *rand.Rand, ledger *Ledger) error
}

// Phase is one segment of the load profile: Poisson arrivals with mean Rate
// ops/second for Duration.
type Phase struct {
	Rate     float64
	Duration time.Duration
}

// Config configures a [Generator].
type Config struct {
	// Seed is the master seed for every generation decision. 0 derives a
	// seed from the wall clock; the resolved value is reported in
	// Result.Seed and must be logged by the caller for reproduction.
	Seed int64
	// Workers bounds scenario concurrency (default 8). When all workers are
	// busy, arrivals queue and the Poisson pacing lags — generation
	// decisions stay reproducible, wall-clock timing does not (see the
	// package doc).
	Workers int
	// Phases is the load profile, executed in order.
	Phases []Phase
}

// Generator runs a weighted scenario mix against a target with Poisson
// arrivals. Build with [NewGenerator].
type Generator struct {
	cfg Config
	mix []Scenario
}

// NewGenerator validates cfg and the mix.
func NewGenerator(cfg Config, mix []Scenario) (*Generator, error) {
	if len(mix) == 0 {
		return nil, errors.New("traffic: mix must contain at least one scenario")
	}
	for _, s := range mix {
		if s.Name == "" || s.Run == nil {
			return nil, fmt.Errorf("traffic: scenario %q must have a name and a Run func", s.Name)
		}
		if s.Weight <= 0 {
			return nil, fmt.Errorf("traffic: scenario %q weight must be positive, got %d", s.Name, s.Weight)
		}
	}
	if len(cfg.Phases) == 0 {
		return nil, errors.New("traffic: at least one phase is required")
	}
	for i, p := range cfg.Phases {
		if p.Rate <= 0 {
			return nil, fmt.Errorf("traffic: phase %d rate must be positive, got %v", i, p.Rate)
		}
		if p.Duration <= 0 {
			return nil, fmt.Errorf("traffic: phase %d duration must be positive, got %v", i, p.Duration)
		}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	return &Generator{cfg: cfg, mix: mix}, nil
}

// op is one scheduled operation: which scenario runs, when it arrives
// (virtual offset from start), and the seed of its private rng. The whole
// schedule is derived from the master seed before any op executes.
type op struct {
	index        int
	scenario     int // index into the mix
	offset       time.Duration
	seed1, seed2 uint64
}

// buildSchedule materializes the full deterministic op schedule for the
// given seed, phases, and mix. Phase op counts come from the exponential
// draws themselves (ops are emitted until the accumulated virtual
// inter-arrival time exceeds the phase duration), never from the wall
// clock — the schedule is a pure function of its arguments.
func buildSchedule(seed int64, phases []Phase, mix []Scenario) []op {
	// The second PCG word is a fixed odd constant (splitmix64's golden-ratio
	// increment) so seed collisions with per-op streams are not a concern.
	//nolint:gosec // G404/G115: deterministic test-traffic generation, not a security context
	master := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15))

	total := 0
	for _, s := range mix {
		total += s.Weight
	}

	var ops []op
	base := time.Duration(0)
	for _, ph := range phases {
		vt := time.Duration(0)
		for {
			gap := time.Duration(master.ExpFloat64() / ph.Rate * float64(time.Second))
			vt += gap
			if vt > ph.Duration {
				break
			}
			pick, w := 0, master.IntN(total)
			for i, s := range mix {
				if w < s.Weight {
					pick = i
					break
				}
				w -= s.Weight
			}
			ops = append(ops, op{
				index:    len(ops),
				scenario: pick,
				offset:   base + vt,
				seed1:    master.Uint64(),
				seed2:    master.Uint64(),
			})
		}
		base += ph.Duration
	}
	return ops
}

// Run executes the schedule: a dispatcher paces ops at their arrival
// offsets, a pool of cfg.Workers goroutines executes them, and every
// completion is recorded in the returned [Result]. Run blocks until every
// dispatched op has completed (or ctx is cancelled — the only error case;
// the partial Result is still returned).
func (g *Generator) Run(ctx context.Context, ledger *Ledger) (*Result, error) {
	seed := g.cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	if ledger == nil {
		ledger = NewLedger()
	}
	ledger.setSeed(seed)

	schedule := buildSchedule(seed, g.cfg.Phases, g.mix)
	result := newResult(seed)

	opsCh := make(chan op)
	var wg sync.WaitGroup
	for w := 0; w < g.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range opsCh {
				sc := g.mix[o.scenario]
				//nolint:gosec // G404: deterministic test-traffic generation, not a security context
				rng := rand.New(rand.NewPCG(o.seed1, o.seed2))
				started := time.Now()
				err := sc.Run(ctx, rng, ledger.forScenario(sc.Name))
				result.record(sc.Name, time.Since(started), err)
			}
		}()
	}

	start := time.Now()
	var runErr error
dispatch:
	for _, o := range schedule {
		if wait := time.Until(start.Add(o.offset)); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				runErr = ctx.Err()
				break dispatch
			}
		}
		select {
		case opsCh <- o: // blocks when all workers are busy: backlog, arrivals lag
		case <-ctx.Done():
			runErr = ctx.Err()
			break dispatch
		}
	}
	close(opsCh)
	wg.Wait()
	result.Elapsed = time.Since(start)
	return result, runErr
}
