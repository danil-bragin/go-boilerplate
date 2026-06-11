// Package traffic is the gateway's reusable scenario pack for the
// platform/testkit/traffic generator: a weighted, adversarial mix of order
// flows (happy path, deterministic declines, edge-validation rejects,
// idempotent retries, idempotency-key mismatch races, read traffic, and SSE
// subscribers) that records the gateway's correctness invariants in a
// traffic.Ledger.
//
// The pack lives WITH the service that owns the API (see
// docs/testing.md §Traffic emulation) and is imported by both the e2e
// traffic test (examples/e2e/traffic_test.go) and the live-stack CLI
// (cmd/trafficgen).
//
// All payload decisions are derived from the per-op rng, so a run is
// reproducible from its seed (generation decisions only — see the
// determinism boundary note in platform/testkit/traffic).
package traffic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	kit "go-boilerplate/platform/testkit/traffic"
)

const (
	// TerminalDeadline is how long Ledger.Verify may poll for an accepted
	// order to reach its terminal status, measured from the Verify call
	// (generation has finished by then; the pipeline is draining).
	TerminalDeadline = 60 * time.Second

	// declineThresholdCents mirrors the payments service's deterministic
	// decline threshold: amounts at or above it end in payment_failed.
	declineThresholdCents = 1_000_000

	// requestTimeout bounds every non-streaming HTTP call.
	requestTimeout = 15 * time.Second

	// sseBudget bounds one SSE subscribe→read-to-terminal flow. Generous:
	// under load the order may take tens of seconds to settle, and without
	// Redis the gateway falls back to store polling.
	sseBudget = 75 * time.Second
)

// currencies is the bounded currency pool drawn from the gateway's
// ISO-4217 allowlist (examples/gateway/internal/api/validate.go).
var currencies = []string{"USD", "EUR", "GBP", "UAH", "PLN"}

// terminalStatuses are the read-model statuses with no further transitions.
var terminalStatuses = map[string]bool{"paid": true, "payment_failed": true, "payment_timeout": true}

// orderBody is the POST /v1/orders request payload.
type orderBody struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// genOrderBody draws a valid below-threshold order payload from rng:
// bounded customer-id pool (50), allowlisted currencies, amount in
// [100, declineThreshold).
func genOrderBody(rng *rand.Rand) orderBody {
	return orderBody{
		CustomerID:  fmt.Sprintf("cust-%03d", rng.IntN(50)),
		AmountCents: 100 + rng.Int64N(declineThresholdCents-100),
		Currency:    currencies[rng.IntN(len(currencies))],
	}
}

// pack carries the shared target wiring and cross-scenario state.
type pack struct {
	base   string
	client *http.Client
	token  string

	// pool feeds the reads scenario with ids accepted by write scenarios.
	poolMu sync.Mutex
	pool   []string
}

const maxPool = 1024

func (p *pack) poolAdd(id string) {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	if len(p.pool) < maxPool {
		p.pool = append(p.pool, id)
	}
}

// poolPick returns a previously accepted id, or "" when none exist yet.
// The index draw consumes rng deterministically, but WHICH ids are in the
// pool depends on completion order — reads are deliberately unledgered.
func (p *pack) poolPick(rng *rand.Rand) string {
	n := rng.IntN(maxPool)
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	if len(p.pool) == 0 {
		return ""
	}
	return p.pool[n%len(p.pool)]
}

// Pack builds the gateway scenario mix for the given base URL. client is
// used for every call (pass one without a global Timeout — SSE streams are
// long-lived; per-request deadlines come from contexts). token, when
// non-empty, is sent as a bearer token (the default e2e/demo stack runs
// with auth disabled — pass "").
func Pack(base string, client *http.Client, token string) []kit.Scenario {
	p := &pack{base: strings.TrimRight(base, "/"), client: client, token: token}
	return []kit.Scenario{
		{Name: "happy", Weight: 70, Run: p.happy},
		{Name: "decline", Weight: 10, Run: p.decline},
		{Name: "invalid", Weight: 5, Run: p.invalid},
		{Name: "idem", Weight: 5, Run: p.idem},
		{Name: "mismatch", Weight: 2, Run: p.mismatch},
		{Name: "reads", Weight: 6, Run: p.reads},
		{Name: "sse", Weight: 2, Run: p.sse},
	}
}

// OrderStatusProbe returns a Ledger.Verify probe reading the order's status
// via GET /v1/orders/{id}. A 404 reports "" (not yet visible — keep
// polling); transport errors and unexpected statuses are probe errors
// (Verify also keeps polling on those, but surfaces the last one).
func OrderStatusProbe(base string, client *http.Client, token string) func(ctx context.Context, id string) (string, error) {
	base = strings.TrimRight(base, "/")
	return func(ctx context.Context, id string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/orders/"+id, http.NoBody)
		if err != nil {
			return "", err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer func() { drain(resp.Body); _ = resp.Body.Close() }()
		switch resp.StatusCode {
		case http.StatusOK:
			var view struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
				return "", err
			}
			return view.Status, nil
		case http.StatusNotFound:
			return "", nil
		default:
			return "", fmt.Errorf("traffic: status probe for %s: HTTP %d", id, resp.StatusCode)
		}
	}
}

// --- scenarios ---------------------------------------------------------

// happy posts a below-threshold order and expects it to settle as paid.
func (p *pack) happy(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	body := genOrderBody(rng)
	res, err := p.postOrder(ctx, "", body)
	if err != nil {
		return kit.CodedError("TRANSPORT", err)
	}
	if res.status != http.StatusAccepted {
		return kit.CodedError(res.codeOrHTTP(), fmt.Errorf("happy: want 202, got %d", res.status))
	}
	ledger.ExpectTerminal(res.orderID, []string{"paid"}, TerminalDeadline)
	p.poolAdd(res.orderID)
	return nil
}

// decline posts an at/above-threshold order: the payments service
// deterministically declines it, so the projection must settle as
// payment_failed.
func (p *pack) decline(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	body := genOrderBody(rng)
	body.AmountCents = declineThresholdCents + rng.Int64N(9_000_000)
	res, err := p.postOrder(ctx, "", body)
	if err != nil {
		return kit.CodedError("TRANSPORT", err)
	}
	if res.status != http.StatusAccepted {
		return kit.CodedError(res.codeOrHTTP(), fmt.Errorf("decline: want 202, got %d", res.status))
	}
	ledger.ExpectTerminal(res.orderID, []string{"payment_failed"}, TerminalDeadline)
	p.poolAdd(res.orderID)
	return nil
}

// invalid posts a payload violating one edge-validation rule and expects a
// 400 problem carrying VALIDATION_FAILED — never an accepted command.
func (p *pack) invalid(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	body := genOrderBody(rng)
	switch rng.IntN(4) {
	case 0:
		body.AmountCents = -1 - rng.Int64N(1000) // gt=0
	case 1:
		body.AmountCents = 0 // gt=0
	case 2:
		body.Currency = "ZZZ" // shape-valid, not in the ISO-4217 allowlist
	default:
		body.CustomerID = "" // required
	}
	opID := fmt.Sprintf("invalid-%016x", rng.Uint64())
	ledger.ExpectRejected(opID, "VALIDATION_FAILED")

	res, err := p.postOrder(ctx, "", body)
	if err != nil {
		ledger.ObserveRejection(opID, "TRANSPORT_ERROR")
		return kit.CodedError("TRANSPORT", err)
	}
	ledger.ObserveRejection(opID, res.codeOrHTTP())
	return nil
}

// idem fires 2–3 CONCURRENT same-key same-body POSTs: a true retry storm.
// Every response must be 202 with the same deterministic (UUIDv5) order id.
func (p *pack) idem(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	key := fmt.Sprintf("idem-%016x", rng.Uint64())
	body := genOrderBody(rng)
	results, err := p.concurrentPosts(ctx, key, replicate(body, 2+rng.IntN(2)))
	if err != nil {
		return kit.CodedError("TRANSPORT", err)
	}

	var errs []error
	winner := ""
	for _, res := range results {
		if res.status != http.StatusAccepted {
			errs = append(errs, fmt.Errorf("idem: same-key same-body retry must 202, got %d (%s)", res.status, res.codeOrHTTP()))
			continue
		}
		ledger.ExpectExactlyOneWinner(key, res.orderID)
		winner = res.orderID
	}
	if winner != "" {
		ledger.ExpectTerminal(winner, []string{"paid"}, TerminalDeadline)
		p.poolAdd(winner)
	}
	return errors.Join(errs...)
}

// mismatch fires 2–3 CONCURRENT same-key DIFFERENT-body POSTs — the
// idempotency-key abuse race. Per the documented contract
// (openapi.yaml Idempotency-Key + gateway Config.PendingAsync):
//
//   - at least one request wins with 202;
//   - each loser gets 409 GATEWAY_IDEMPOTENCY_BODY_MISMATCH, OR — within
//     the replica-lag / async-pending window — 202 carrying the winner's
//     id (only the 409 signal is lost, never the dedup);
//   - one key must NEVER yield two different order ids. That hard
//     invariant is what the winner group asserts.
func (p *pack) mismatch(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	key := fmt.Sprintf("mismatch-%016x", rng.Uint64())
	n := 2 + rng.IntN(2)
	bodies := make([]orderBody, n)
	for i := range bodies {
		bodies[i] = genOrderBody(rng)
		// Force the bodies pairwise-different in amount (still below the
		// decline threshold) so every loser is a genuine mismatch.
		bodies[i].AmountCents = 100 + (bodies[i].AmountCents-100)/int64(n)*int64(n) + int64(i)
	}
	results, err := p.concurrentPosts(ctx, key, bodies)
	if err != nil {
		return kit.CodedError("TRANSPORT", err)
	}

	var errs []error
	winner := ""
	for i, res := range results {
		switch res.status {
		case http.StatusAccepted:
			ledger.ExpectExactlyOneWinner(key, res.orderID)
			winner = res.orderID
		case http.StatusConflict:
			opID := fmt.Sprintf("%s-loser-%d", key, i)
			ledger.ExpectRejected(opID, "GATEWAY_IDEMPOTENCY_BODY_MISMATCH")
			ledger.ObserveRejection(opID, res.codeOrHTTP())
			ledger.ObserveLoser(key)
		default:
			errs = append(errs, fmt.Errorf("mismatch: want 202 or 409, got %d (%s)", res.status, res.codeOrHTTP()))
		}
	}
	if winner != "" {
		ledger.ExpectTerminal(winner, []string{"paid"}, TerminalDeadline)
	}
	return errors.Join(errs...)
}

// reads drives the read path (unledgered — see the plan: reads assert
// response shape inline, they create no expectations).
func (p *pack) reads(ctx context.Context, rng *rand.Rand, _ *kit.Ledger) error {
	switch rng.IntN(3) {
	case 0: // LIST with a random page size
		limit := 1 + rng.IntN(100)
		status, _, err := p.get(ctx, fmt.Sprintf("/v1/orders?limit=%d", limit))
		if err != nil {
			return kit.CodedError("TRANSPORT", err)
		}
		if status != http.StatusOK {
			return kit.CodedError(fmt.Sprintf("HTTP_%d", status), fmt.Errorf("reads: list want 200, got %d", status))
		}
	case 1: // GET a known accepted id
		id := p.poolPick(rng)
		if id == "" {
			return nil // nothing accepted yet (run just started)
		}
		status, _, err := p.get(ctx, "/v1/orders/"+id)
		if err != nil {
			return kit.CodedError("TRANSPORT", err)
		}
		// 404 is tolerated ONLY because GATEWAY_PENDING_ASYNC=true defers
		// the pending row to a batched flush (documented in config.go);
		// with the sync default the row exists before the 202 returns.
		if status != http.StatusOK && status != http.StatusNotFound {
			return kit.CodedError(fmt.Sprintf("HTTP_%d", status), fmt.Errorf("reads: get %s want 200/404, got %d", id, status))
		}
	default: // GET an unknown id → the coded 404, never a 5xx
		id := rngUUID(rng)
		status, code, err := p.get(ctx, "/v1/orders/"+id)
		if err != nil {
			return kit.CodedError("TRANSPORT", err)
		}
		if status != http.StatusNotFound || code != "GATEWAY_ORDER_NOT_FOUND" {
			return kit.CodedError(fmt.Sprintf("HTTP_%d", status),
				fmt.Errorf("reads: unknown id want 404 GATEWAY_ORDER_NOT_FOUND, got %d %q", status, code))
		}
	}
	return nil
}

// sse posts an order and subscribes to its event stream
// (GET /v1/orders/{id}/events). Two client behaviours, picked from rng:
//
//   - early-drop: read the first event, then disconnect — exercises the
//     server's cleanup of abandoned streams;
//   - read-to-terminal with resume: read the first event, disconnect,
//     reconnect with Last-Event-ID, and read on until a terminal status —
//     exercising monotone event ids and Last-Event-ID resume.
func (p *pack) sse(ctx context.Context, rng *rand.Rand, ledger *kit.Ledger) error {
	earlyDrop := rng.IntN(2) == 0
	body := genOrderBody(rng)
	res, err := p.postOrder(ctx, "", body)
	if err != nil {
		return kit.CodedError("TRANSPORT", err)
	}
	if res.status != http.StatusAccepted {
		return kit.CodedError(res.codeOrHTTP(), fmt.Errorf("sse: want 202, got %d", res.status))
	}
	ledger.ExpectTerminal(res.orderID, []string{"paid"}, TerminalDeadline)
	p.poolAdd(res.orderID)

	ctx, cancel := context.WithTimeout(ctx, sseBudget)
	defer cancel()

	// First connection: read until the first event.
	lastID, status, err := p.streamEvents(ctx, res.orderID, -1, stopAfterFirst)
	if err != nil {
		return kit.CodedError("SSE", fmt.Errorf("sse: first read for %s: %w", res.orderID, err))
	}
	if earlyDrop || terminalStatuses[status] {
		return nil
	}

	// Resume with Last-Event-ID: the server must only send events with a
	// GREATER id (no regressions), up to the terminal status.
	_, status, err = p.streamEvents(ctx, res.orderID, lastID, stopAtTerminal)
	if err != nil {
		return kit.CodedError("SSE", fmt.Errorf("sse: resume read for %s: %w", res.orderID, err))
	}
	if !terminalStatuses[status] {
		return kit.CodedError("SSE", fmt.Errorf("sse: stream for %s ended before a terminal status (last %q)", res.orderID, status))
	}
	return nil
}

// --- SSE client --------------------------------------------------------

type sseStop int

const (
	stopAfterFirst sseStop = iota
	stopAtTerminal
)

// streamEvents opens the order's SSE stream (resuming after lastEventID
// when ≥ 0) and reads events until the stop condition. It returns the last
// event id and status seen. Event ids must be strictly greater than
// lastEventID and strictly increasing — a regression is an error.
func (p *pack) streamEvents(ctx context.Context, orderID string, lastEventID int, stop sseStop) (last int, status string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/v1/orders/"+orderID+"/events", http.NoBody)
	if err != nil {
		return lastEventID, "", err
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID >= 0 {
		req.Header.Set("Last-Event-ID", strconv.Itoa(lastEventID))
	}
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return lastEventID, "", err
	}
	// NEVER drain an event-stream body: it is a live, open stream — a
	// "polite" drain-before-close would block until the server closes it
	// (i.e. until the request context expires). Closing without draining
	// tears the connection down immediately; SSE connections are not
	// reusable anyway.
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return lastEventID, "", fmt.Errorf("stream status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		return lastEventID, "", fmt.Errorf("stream content-type %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	eventID := -1
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, ":"): // heartbeat comment
		case strings.HasPrefix(line, "id:"):
			if eventID, err = strconv.Atoi(strings.TrimSpace(line[len("id:"):])); err != nil {
				return lastEventID, status, fmt.Errorf("bad event id %q", line)
			}
		case strings.HasPrefix(line, "data:"):
			var payload struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(line[len("data:"):])), &payload); err != nil {
				return lastEventID, status, fmt.Errorf("bad event data %q: %w", line, err)
			}
			status = payload.Status
		case line == "" && eventID >= 0: // dispatch
			if eventID <= lastEventID {
				return lastEventID, status, fmt.Errorf("event id regression: got %d after %d", eventID, lastEventID)
			}
			lastEventID = eventID
			eventID = -1
			if stop == stopAfterFirst || terminalStatuses[status] {
				return lastEventID, status, nil
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil && !errors.Is(scanErr, context.Canceled) {
		return lastEventID, status, scanErr
	}
	return lastEventID, status, nil
}

// --- HTTP plumbing -----------------------------------------------------

// postResult is one POST /v1/orders outcome.
type postResult struct {
	status  int
	orderID string
	code    string // problem+json code, when present
}

// codeOrHTTP returns the problem code when the response carried one, else
// a synthetic HTTP_<status> bucket.
func (r postResult) codeOrHTTP() string {
	if r.code != "" {
		return r.code
	}
	return fmt.Sprintf("HTTP_%d", r.status)
}

// postOrder POSTs one order, optionally with an Idempotency-Key.
func (p *pack) postOrder(ctx context.Context, idemKey string, body orderBody) (postResult, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	payload, err := json.Marshal(body)
	if err != nil {
		return postResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+"/v1/orders", bytes.NewReader(payload))
	if err != nil {
		return postResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return postResult{}, err
	}
	defer func() { drain(resp.Body); _ = resp.Body.Close() }()

	res := postResult{status: resp.StatusCode}
	var parsed struct {
		OrderID string `json:"order_id"`
		Code    string `json:"code"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err == nil {
		res.orderID = parsed.OrderID
		res.code = parsed.Code
	}
	if res.status == http.StatusAccepted && res.orderID == "" {
		return res, errors.New("202 without an order_id")
	}
	return res, nil
}

// concurrentPosts fires one POST per body with the same Idempotency-Key,
// all in flight at once, and returns the per-request outcomes. A transport
// error on any request fails the whole group.
func (p *pack) concurrentPosts(ctx context.Context, key string, bodies []orderBody) ([]postResult, error) {
	results := make([]postResult, len(bodies))
	errs := make([]error, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = p.postOrder(ctx, key, body)
		}()
	}
	wg.Wait()
	return results, errors.Join(errs...)
}

// get performs one GET and returns (status, problem code when present).
func (p *pack) get(ctx context.Context, path string) (status int, code string, err error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+path, http.NoBody)
	if err != nil {
		return 0, "", err
	}
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { drain(resp.Body); _ = resp.Body.Close() }()
	var parsed struct {
		Code string `json:"code"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed.Code, nil
}

// replicate returns n copies of body.
func replicate(body orderBody, n int) []orderBody {
	out := make([]orderBody, n)
	for i := range out {
		out[i] = body
	}
	return out
}

// rngUUID derives a v4-shaped UUID from rng (deterministic per seed,
// unlike uuid.New).
func rngUUID(rng *rand.Rand) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], rng.Uint64())
	binary.BigEndian.PutUint64(b[8:16], rng.Uint64())
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	u, _ := uuid.FromBytes(b[:])
	return u.String()
}

// drain consumes (a bounded amount of) a response body before close so the
// underlying connection can be reused.
func drain(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
}
