// Command trafficgen drives the gateway scenario pack
// (examples/gateway/traffic) against a LIVE stack through the seeded
// platform/testkit/traffic generator: correctness under load with a
// reproducible adversarial mix, complementing k6 (external SLO/perf — see
// docs/operations.md §Load testing).
//
// Usage:
//
//	just traffic                                  # 20 rps for 30s against localhost:8080
//	just traffic --rate 50 --duration 1m
//	just traffic --phases "10rps:5s,40rps:20s,80rps:5s"
//	just traffic --mix happy=70,decline=10,invalid=5,idem=5,mismatch=2,reads=6,sse=2
//	just traffic --seed 1718041200000000000      # replay a previous run
//	TOKEN=$(just token) just traffic --token "$TOKEN"   # auth-enabled stack
//
// The resolved seed is always printed: a run's generation decisions
// (scenario sequence, payloads, arrival gaps) replay exactly under the same
// seed (see the determinism boundary in platform/testkit/traffic).
//
// After generation, every recorded invariant is verified by polling the
// gateway read API (terminal statuses; idempotency winner groups; rejection
// codes). The orders-DB row-count cross-check from the e2e traffic test is
// NOT available here — the CLI only has the HTTP edge. Exit codes: 0 = all
// invariants hold, 1 = violations (or scenario failures), 2 = usage error.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	gwtraffic "go-boilerplate/examples/gateway/traffic"
	kit "go-boilerplate/platform/testkit/traffic"
)

func main() {
	os.Exit(realMain())
}

// realMain runs the CLI and returns the process exit code; keeping os.Exit
// out of the deferring function so cleanup (signal stop) always runs.
func realMain() int {
	var (
		baseURL  = flag.String("base-url", "http://localhost:8080", "gateway base URL")
		rate     = flag.Float64("rate", 20, "mean request rate in rps (single phase; ignored when --phases is set)")
		duration = flag.Duration("duration", 30*time.Second, "run duration (single phase; ignored when --phases is set)")
		workers  = flag.Int("workers", 16, "max concurrent scenario executions")
		seed     = flag.Int64("seed", 0, "master seed (0 = derive from time; the resolved seed is always printed)")
		phases   = flag.String("phases", "", `load profile, e.g. "10rps:5s,40rps:20s,80rps:5s" (overrides --rate/--duration)`)
		mix      = flag.String("mix", "", `scenario weights, e.g. "happy=70,decline=10,sse=0" (0 drops a scenario; unmentioned ones keep pack weights)`)
		token    = flag.String("token", "", "bearer token for auth-enabled stacks (optional; default stack runs auth-disabled)")
	)
	flag.Parse()

	profile := []kit.Phase{{Rate: *rate, Duration: *duration}}
	if *phases != "" {
		parsed, err := parsePhases(*phases)
		if err != nil {
			fmt.Fprintf(os.Stderr, "trafficgen: %v\n", err)
			return 2
		}
		profile = parsed
	}

	scenarios := gwtraffic.Pack(*baseURL, newClient(), *token)
	if *mix != "" {
		weights, err := parseMix(*mix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "trafficgen: %v\n", err)
			return 2
		}
		if scenarios, err = applyMix(scenarios, weights); err != nil {
			fmt.Fprintf(os.Stderr, "trafficgen: %v\n", err)
			return 2
		}
	}

	gen, err := kit.NewGenerator(kit.Config{Seed: *seed, Workers: *workers, Phases: profile}, scenarios)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trafficgen: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ledger := kit.NewLedger()
	result, runErr := gen.Run(ctx, ledger)
	fmt.Printf("%s\n", result)
	fmt.Printf("replay this run: --seed %d\n\n", result.Seed)
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "trafficgen: run aborted: %v\n", runErr)
		return 1
	}

	fmt.Printf("verifying invariants (terminal deadline %s)...\n", gwtraffic.TerminalDeadline)
	violations := ledger.Verify(ctx, kit.Probes{
		OrderStatus: gwtraffic.OrderStatusProbe(*baseURL, newClient(), *token),
	})
	for _, v := range violations {
		fmt.Printf("VIOLATION %s\n", v)
	}
	if len(violations) > 0 {
		fmt.Printf("\n%d invariant violation(s) — replay with --seed %d\n", len(violations), result.Seed)
		return 1
	}
	fmt.Println("all invariants hold")
	if result.TotalFailed() > 0 {
		fmt.Fprintf(os.Stderr, "trafficgen: %d scenario failure(s) — see the errors column above\n", result.TotalFailed())
		return 1
	}
	return 0
}

// newClient builds the HTTP client for generation and probing: no global
// Timeout (SSE streams are long-lived; the pack applies per-request context
// deadlines) and a roomy idle pool for sustained rates.
func newClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	return &http.Client{Transport: transport}
}

// parsePhases parses "10rps:5s,40rps:20s" (the "rps" suffix is optional)
// into the generator's load profile.
func parsePhases(s string) ([]kit.Phase, error) {
	parts := strings.Split(s, ",")
	phases := make([]kit.Phase, 0, len(parts))
	for _, part := range parts {
		rateStr, durStr, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf(`phase %q: want "<rate>rps:<duration>"`, part)
		}
		rate, err := strconv.ParseFloat(strings.TrimSuffix(rateStr, "rps"), 64)
		if err != nil {
			return nil, fmt.Errorf("phase %q: bad rate: %w", part, err)
		}
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			return nil, fmt.Errorf("phase %q: bad duration: %w", part, err)
		}
		if rate <= 0 || dur <= 0 {
			return nil, fmt.Errorf("phase %q: rate and duration must be positive", part)
		}
		phases = append(phases, kit.Phase{Rate: rate, Duration: dur})
	}
	return phases, nil
}

// parseMix parses "happy=70,decline=10,sse=0" into scenario weights.
func parseMix(s string) (map[string]int, error) {
	weights := make(map[string]int)
	for _, part := range strings.Split(s, ",") {
		name, weightStr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" {
			return nil, fmt.Errorf(`mix entry %q: want "<scenario>=<weight>"`, part)
		}
		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			return nil, fmt.Errorf("mix entry %q: bad weight: %w", part, err)
		}
		if weight < 0 {
			return nil, fmt.Errorf("mix entry %q: weight must be ≥ 0 (0 drops the scenario)", part)
		}
		weights[name] = weight
	}
	return weights, nil
}

// applyMix reweights the pack: mentioned scenarios take the given weight
// (0 removes them), unmentioned ones keep their pack defaults. Unknown
// names are an error — silently ignoring a typo would skew the run.
func applyMix(scenarios []kit.Scenario, weights map[string]int) ([]kit.Scenario, error) {
	known := make(map[string]bool, len(scenarios))
	for _, s := range scenarios {
		known[s.Name] = true
	}
	for name := range weights {
		if !known[name] {
			return nil, fmt.Errorf("unknown scenario %q in --mix (have: %s)", name, scenarioNames(scenarios))
		}
	}
	out := make([]kit.Scenario, 0, len(scenarios))
	for _, s := range scenarios {
		if w, ok := weights[s.Name]; ok {
			if w == 0 {
				continue
			}
			s.Weight = w
		}
		out = append(out, s)
	}
	return out, nil
}

func scenarioNames(scenarios []kit.Scenario) string {
	names := make([]string, 0, len(scenarios))
	for _, s := range scenarios {
		names = append(names, s.Name)
	}
	return strings.Join(names, ", ")
}
