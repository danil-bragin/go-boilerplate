package traffic

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// defaultPollInterval paces Verify's probe polling when
// Probes.PollInterval is zero.
const defaultPollInterval = 250 * time.Millisecond

// Probes are the read-side hooks [Ledger.Verify] uses to observe the target.
// Either probe may be nil: a nil OrderStatus turns every terminal
// expectation into a violation (you asked for terminal checks but gave
// Verify no way to observe them); a nil CountOrders skips the row-count
// cross-check.
type Probes struct {
	// OrderStatus returns the target's current status for an id. Errors and
	// not-yet-visible ids ("" status) are treated as "not yet" and polled
	// again until the expectation's deadline.
	OrderStatus func(ctx context.Context, orderID string) (string, error)
	// CountOrders returns how many of the given ids exist as rows in the
	// system of record. Verify expects exactly len(ids) — every accepted id
	// exists, and (because ids are primary keys) none was duplicated.
	CountOrders func(ctx context.Context, orderIDs []string) (int, error)
	// PollInterval overrides the polling cadence (default 250ms).
	PollInterval time.Duration
}

// Violation is one failed invariant, carrying enough context to debug and
// to replay: the scenario that recorded the expectation and the run seed.
type Violation struct {
	Kind     string // "terminal" | "winner" | "rejected" | "orders-count"
	Scenario string
	ID       string // order id, winner group, or op id
	Expected string
	Observed string
	Seed     int64
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] scenario=%s id=%s expected=%s observed=%s (seed=%d)",
		v.Kind, v.Scenario, v.ID, v.Expected, v.Observed, v.Seed)
}

type terminalExp struct {
	scenario string
	orderID  string
	allowed  []string
	within   time.Duration
}

type winnerGroup struct {
	scenario string
	ids      []string // every accepted (e.g. 202) order id, duplicates kept
	losers   int      // rejected (e.g. 409) responses — bookkeeping only
}

type rejectExp struct {
	scenario string
	wantCode string
}

type ledgerState struct {
	mu        sync.Mutex
	seed      int64
	terminals []terminalExp
	winners   map[string]*winnerGroup
	rejects   map[string]rejectExp
	observed  map[string]string
}

// Ledger records invariant expectations during a run and verifies them
// afterwards. It is safe for concurrent use by scenario goroutines.
//
// Handles returned by the generator carry the recording scenario's name so
// violations are attributable; all handles share one underlying state.
type Ledger struct {
	s        *ledgerState
	scenario string
}

// NewLedger builds an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{s: &ledgerState{
		winners:  make(map[string]*winnerGroup),
		rejects:  make(map[string]rejectExp),
		observed: make(map[string]string),
	}}
}

// forScenario returns a handle whose recordings are attributed to the named
// scenario. State is shared with the parent.
func (l *Ledger) forScenario(name string) *Ledger {
	return &Ledger{s: l.s, scenario: name}
}

func (l *Ledger) setSeed(seed int64) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.seed = seed
}

// ExpectTerminal records that orderID must reach one of the allowed
// statuses within the given duration of the Verify call (NOT of the
// recording — Verify runs after generation, when the pipeline is draining).
func (l *Ledger) ExpectTerminal(orderID string, allowed []string, within time.Duration) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.terminals = append(l.s.terminals, terminalExp{
		scenario: l.scenario, orderID: orderID, allowed: allowed, within: within,
	})
}

// ExpectExactlyOneWinner records one ACCEPTED response (e.g. a 202) for an
// idempotency group: call it once per accepted response with the id the
// target returned. Verify requires every group to have at least one
// accepted response and exactly ONE distinct id across them — two distinct
// ids for one group is the unconditional violation (a duplicated order).
func (l *Ledger) ExpectExactlyOneWinner(group, orderID string) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	g := l.group(group)
	g.ids = append(g.ids, orderID)
}

// ObserveLoser records one REJECTED response (e.g. a 409 body-mismatch) for
// an idempotency group — bookkeeping that keeps the group alive so a group
// consisting only of losers (no winner at all) is reported as a violation.
func (l *Ledger) ObserveLoser(group string) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.group(group).losers++
}

// group returns (creating if needed) the named winner group. Callers hold mu.
func (l *Ledger) group(name string) *winnerGroup {
	g := l.s.winners[name]
	if g == nil {
		g = &winnerGroup{scenario: l.scenario}
		l.s.winners[name] = g
	}
	return g
}

// ExpectRejected records that the op identified by opID must have been
// rejected with the given code. Pair with [Ledger.ObserveRejection].
func (l *Ledger) ExpectRejected(opID, code string) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.rejects[opID] = rejectExp{scenario: l.scenario, wantCode: code}
}

// ObserveRejection records the code the target actually answered for opID
// (use a synthetic code like "HTTP_500" for non-problem responses).
func (l *Ledger) ObserveRejection(opID, gotCode string) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.observed[opID] = gotCode
}

// Verify checks every recorded expectation and returns the violations
// (empty slice = pass):
//
//   - rejected expectations are matched against their observations
//     (no probe involved);
//   - winner groups must each contain ≥1 accepted id and exactly one
//     DISTINCT id;
//   - terminal expectations are polled via Probes.OrderStatus until
//     satisfied or their per-expectation deadline (measured from the Verify
//     call) elapses;
//   - finally, Probes.CountOrders (when non-nil) must report exactly one
//     row per unique accepted id.
//
// Verify returns early when ctx is cancelled; unresolved terminal
// expectations are then reported as violations.
func (l *Ledger) Verify(ctx context.Context, probes Probes) []Violation {
	l.s.mu.Lock()
	seed := l.s.seed
	terminals := make([]terminalExp, len(l.s.terminals))
	copy(terminals, l.s.terminals)
	winners := make(map[string]*winnerGroup, len(l.s.winners))
	for k, v := range l.s.winners {
		cp := *v
		winners[k] = &cp
	}
	rejects := make(map[string]rejectExp, len(l.s.rejects))
	for k, v := range l.s.rejects {
		rejects[k] = v
	}
	observed := make(map[string]string, len(l.s.observed))
	for k, v := range l.s.observed {
		observed[k] = v
	}
	l.s.mu.Unlock()

	var violations []Violation

	// 1. Rejections: expectation ↔ observation, no probe.
	for opID, exp := range sortedKeys(rejects) {
		got, ok := observed[opID]
		switch {
		case !ok:
			violations = append(violations, Violation{
				Kind: "rejected", Scenario: exp.scenario, ID: opID,
				Expected: exp.wantCode, Observed: "<no rejection observed>", Seed: seed,
			})
		case got != exp.wantCode:
			violations = append(violations, Violation{
				Kind: "rejected", Scenario: exp.scenario, ID: opID,
				Expected: exp.wantCode, Observed: got, Seed: seed,
			})
		}
	}

	// 2. Winner groups: ≥1 accepted, exactly one distinct id.
	for group, g := range sortedKeys(winners) {
		distinct := uniqueStrings(g.ids)
		switch {
		case len(distinct) == 0:
			violations = append(violations, Violation{
				Kind: "winner", Scenario: g.scenario, ID: group,
				Expected: "at least one accepted response",
				Observed: fmt.Sprintf("0 accepted, %d rejected", g.losers), Seed: seed,
			})
		case len(distinct) > 1:
			violations = append(violations, Violation{
				Kind: "winner", Scenario: g.scenario, ID: group,
				Expected: "exactly one distinct order id",
				Observed: fmt.Sprintf("%d distinct ids: %s", len(distinct), strings.Join(distinct, ", ")),
				Seed:     seed,
			})
		}
	}

	// 3. Terminal expectations: poll until satisfied or deadline.
	violations = append(violations, l.verifyTerminals(ctx, probes, terminals, seed)...)

	// 4. Row-count cross-check over every unique accepted id.
	if probes.CountOrders != nil {
		ids := acceptedIDs(terminals, winners)
		if len(ids) > 0 {
			n, err := probes.CountOrders(ctx, ids)
			switch {
			case err != nil:
				violations = append(violations, Violation{
					Kind: "orders-count", ID: "<all accepted ids>",
					Expected: strconv.Itoa(len(ids)),
					Observed: "probe error: " + err.Error(), Seed: seed,
				})
			case n != len(ids):
				violations = append(violations, Violation{
					Kind: "orders-count", ID: "<all accepted ids>",
					Expected: strconv.Itoa(len(ids)),
					Observed: strconv.Itoa(n), Seed: seed,
				})
			}
		}
	}

	return violations
}

// verifyTerminals polls every terminal expectation until it is satisfied or
// its deadline (Verify start + within) passes.
func (l *Ledger) verifyTerminals(ctx context.Context, probes Probes, terminals []terminalExp, seed int64) []Violation {
	if len(terminals) == 0 {
		return nil
	}

	var violations []Violation
	if probes.OrderStatus == nil {
		for _, exp := range terminals {
			violations = append(violations, Violation{
				Kind: "terminal", Scenario: exp.scenario, ID: exp.orderID,
				Expected: strings.Join(exp.allowed, "|"),
				Observed: "<no OrderStatus probe configured>", Seed: seed,
			})
		}
		return violations
	}

	interval := probes.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	start := time.Now()
	type pending struct {
		exp      terminalExp
		deadline time.Time
		last     string // last observed status or probe error
	}
	open := make([]*pending, 0, len(terminals))
	for _, exp := range terminals {
		open = append(open, &pending{exp: exp, deadline: start.Add(exp.within), last: "<never observed>"})
	}

	// Probes run with bounded parallelism: a serial pass over thousands of
	// expectations (CLI scale) would erode every per-expectation deadline to
	// one or two polls. 16 in flight keeps probe pressure trivial for any
	// real target while making pass time ~O(n/16).
	const probeParallelism = 16

	for len(open) > 0 {
		sem := make(chan struct{}, probeParallelism)
		var wg sync.WaitGroup
		satisfied := make([]atomic.Bool, len(open))
		for i, p := range open {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				status, err := probes.OrderStatus(ctx, p.exp.orderID)
				if err != nil && time.Now().After(p.deadline) && ctx.Err() == nil {
					// A transient probe error on the FINAL poll must not turn
					// a satisfied expectation into a violation: one immediate
					// grace retry before the deadline check below.
					status, err = probes.OrderStatus(ctx, p.exp.orderID)
				}
				switch {
				case err != nil:
					p.last = "probe error: " + err.Error()
				case status != "":
					p.last = status
				}
				if err == nil && slices.Contains(p.exp.allowed, status) {
					satisfied[i].Store(true)
				}
			}()
		}
		wg.Wait()

		next := open[:0]
		for i, p := range open {
			if satisfied[i].Load() {
				continue // satisfied
			}
			if time.Now().After(p.deadline) || ctx.Err() != nil {
				violations = append(violations, Violation{
					Kind: "terminal", Scenario: p.exp.scenario, ID: p.exp.orderID,
					Expected: strings.Join(p.exp.allowed, "|"),
					Observed: p.last, Seed: seed,
				})
				continue
			}
			next = append(next, p)
		}
		open = next
		if len(open) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			// Next pass reports everything still open as a violation.
		case <-time.After(interval):
		}
	}
	return violations
}

// acceptedIDs returns the sorted unique order ids accepted during the run:
// every terminal expectation plus every winner-group id.
func acceptedIDs(terminals []terminalExp, winners map[string]*winnerGroup) []string {
	seen := make(map[string]struct{})
	for _, exp := range terminals {
		seen[exp.orderID] = struct{}{}
	}
	for _, g := range winners {
		for _, id := range g.ids {
			seen[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// sortedKeys iterates a map in sorted-key order so violation output is
// deterministic.
func sortedKeys[V any](m map[string]V) func(yield func(string, V) bool) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return func(yield func(string, V) bool) {
		for _, k := range keys {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}
