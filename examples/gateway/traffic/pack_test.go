package traffic

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kit "go-boilerplate/platform/testkit/traffic"
)

func newRng(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, 0)) //nolint:gosec // G404: test rng
}

func scenarioByName(t *testing.T, scenarios []kit.Scenario, name string) kit.Scenario {
	t.Helper()
	for _, s := range scenarios {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("scenario %q not found in pack", name)
	return kit.Scenario{}
}

// stubProbes verifies the ledger with an always-terminal status probe.
func stubProbes(status string) kit.Probes {
	return kit.Probes{
		OrderStatus:  func(context.Context, string) (string, error) { return status, nil },
		PollInterval: 5 * time.Millisecond,
	}
}

func TestPack_NamesAndWeights(t *testing.T) {
	scenarios := Pack("http://example.invalid", http.DefaultClient, "")
	want := map[string]int{
		"happy": 70, "decline": 10, "invalid": 5, "idem": 5, "mismatch": 2, "reads": 6, "sse": 2,
	}
	require.Len(t, scenarios, len(want))
	for name, weight := range want {
		s := scenarioByName(t, scenarios, name)
		assert.Equal(t, weight, s.Weight, "weight of %s", name)
		assert.NotNil(t, s.Run)
	}
}

func TestGenOrderBody_DeterministicFromSeed(t *testing.T) {
	a, b := newRng(7), newRng(7)
	for i := 0; i < 20; i++ {
		require.Equal(t, genOrderBody(a), genOrderBody(b), "draw %d", i)
	}
	// Bounded cardinality + below the decline threshold.
	rng := newRng(3)
	for i := 0; i < 200; i++ {
		body := genOrderBody(rng)
		assert.True(t, strings.HasPrefix(body.CustomerID, "cust-"), "customer pool")
		assert.Less(t, body.AmountCents, int64(declineThresholdCents))
		assert.Positive(t, body.AmountCents)
		assert.Contains(t, currencies, body.Currency)
	}
}

// fakeGateway is a minimal in-memory stand-in for POST /v1/orders that
// reproduces the real idempotency semantics: the order id is deterministic
// per Idempotency-Key, a same-key same-body retry returns 202 with the same
// id, and a same-key different-body request returns 409 with the documented
// code — unless absorb202 is set, which models the documented replica-lag /
// async-pending window (202 with the WINNER's id instead of the 409 signal).
type fakeGateway struct {
	mu       sync.Mutex
	byKey    map[string]orderBody // first body seen per key
	idByKey  map[string]string
	absorb   bool // 409s absorbed into 202-with-winner-id (documented window)
	buggy    bool // VIOLATION mode: fresh id per request, even for one key
	statuses int  // requests served
}

func (f *fakeGateway) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/orders" {
			http.NotFound(w, r)
			return
		}
		var body orderBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.statuses++

		key := r.Header.Get("Idempotency-Key")
		if key == "" || f.buggy {
			writeJSON(w, http.StatusAccepted, map[string]string{"order_id": uuid.New().String()})
			return
		}
		if f.idByKey == nil {
			f.idByKey = map[string]string{}
			f.byKey = map[string]orderBody{}
		}
		id, seen := f.idByKey[key]
		if !seen {
			id = uuid.New().String()
			f.idByKey[key] = id
			f.byKey[key] = body
		}
		if seen && f.byKey[key] != body && !f.absorb {
			w.Header().Set("Content-Type", "application/problem+json")
			writeJSON(w, http.StatusConflict, map[string]any{
				"status": 409, "code": "GATEWAY_IDEMPOTENCY_BODY_MISMATCH",
			})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"order_id": id})
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestMismatchScenario_CorrectServerPassesVerify(t *testing.T) {
	fg := &fakeGateway{}
	srv := httptest.NewServer(fg.handler())
	defer srv.Close()

	scenarios := Pack(srv.URL, srv.Client(), "")
	mismatch := scenarioByName(t, scenarios, "mismatch")

	ledger := kit.NewLedger()
	for seed := uint64(1); seed <= 5; seed++ {
		require.NoError(t, mismatch.Run(context.Background(), newRng(seed), ledger))
	}
	assert.Empty(t, ledger.Verify(context.Background(), stubProbes("paid")))
}

func TestMismatchScenario_AbsorbedWithinWindowPassesVerify(t *testing.T) {
	// The documented tolerance: a mismatch inside the replica-lag /
	// async-pending window loses only the 409 signal — the loser gets 202
	// with the WINNER's deterministic id. That must NOT be a violation.
	fg := &fakeGateway{absorb: true}
	srv := httptest.NewServer(fg.handler())
	defer srv.Close()

	mismatch := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "mismatch")
	ledger := kit.NewLedger()
	for seed := uint64(1); seed <= 5; seed++ {
		require.NoError(t, mismatch.Run(context.Background(), newRng(seed), ledger))
	}
	assert.Empty(t, ledger.Verify(context.Background(), stubProbes("paid")))
}

func TestMismatchScenario_TwoDistinctIDsIsViolation(t *testing.T) {
	// The hard invariant: one idempotency key must NEVER yield two
	// different order ids. A buggy server minting fresh ids per request
	// must produce a winner violation.
	fg := &fakeGateway{buggy: true}
	srv := httptest.NewServer(fg.handler())
	defer srv.Close()

	mismatch := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "mismatch")
	ledger := kit.NewLedger()
	require.NoError(t, mismatch.Run(context.Background(), newRng(1), ledger))

	violations := ledger.Verify(context.Background(), stubProbes("paid"))
	require.NotEmpty(t, violations)
	found := false
	for _, v := range violations {
		if v.Kind == "winner" {
			found = true
			assert.Contains(t, v.Expected, "exactly one distinct order id")
		}
	}
	assert.True(t, found, "expected a winner violation, got: %v", violations)
}

func TestIdemScenario_SameKeySameBodyAllWin(t *testing.T) {
	fg := &fakeGateway{}
	srv := httptest.NewServer(fg.handler())
	defer srv.Close()

	idem := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "idem")
	ledger := kit.NewLedger()
	for seed := uint64(1); seed <= 5; seed++ {
		require.NoError(t, idem.Run(context.Background(), newRng(seed), ledger))
	}
	assert.Empty(t, ledger.Verify(context.Background(), stubProbes("paid")))
}

func TestInvalidScenario_RejectionCodeBookkeeping(t *testing.T) {
	t.Run("server answers VALIDATION_FAILED", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/problem+json")
			writeJSON(w, http.StatusBadRequest, map[string]any{"status": 400, "code": "VALIDATION_FAILED"})
		}))
		defer srv.Close()

		invalid := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "invalid")
		ledger := kit.NewLedger()
		require.NoError(t, invalid.Run(context.Background(), newRng(1), ledger))
		assert.Empty(t, ledger.Verify(context.Background(), kit.Probes{}))
	})

	t.Run("server accepting garbage is a violation", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusAccepted, map[string]string{"order_id": uuid.New().String()})
		}))
		defer srv.Close()

		invalid := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "invalid")
		ledger := kit.NewLedger()
		_ = invalid.Run(context.Background(), newRng(1), ledger)

		violations := ledger.Verify(context.Background(), kit.Probes{})
		require.Len(t, violations, 1)
		assert.Equal(t, "rejected", violations[0].Kind)
		assert.Equal(t, "VALIDATION_FAILED", violations[0].Expected)
	})
}

func TestSSEScenario_ReadsToTerminal(t *testing.T) {
	// Fake gateway: POST mints an id; the events route streams
	// created → paid as SSE frames with monotone ids.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]string{"order_id": uuid.New().String()})
	})
	mux.HandleFunc("GET /v1/orders/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		fl := w.(http.Flusher)
		last := -1
		if v := r.Header.Get("Last-Event-ID"); v != "" {
			fmt.Sscanf(v, "%d", &last)
		}
		if last < 1 {
			fmt.Fprintf(w, ": hb\n\nid: 1\ndata: {\"status\":\"created\"}\n\n")
			fl.Flush()
		}
		fmt.Fprintf(w, "id: 2\ndata: {\"status\":\"paid\"}\n\n")
		fl.Flush()
		// Hold the stream OPEN after the terminal event, like the real
		// gateway (which keeps heartbeating): the client must return as
		// soon as it has seen a terminal status / its first event, never
		// wait for the server to close (regression guard for the
		// drain-blocks-on-live-stream bug).
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	sse := scenarioByName(t, Pack(srv.URL, srv.Client(), ""), "sse")
	ledger := kit.NewLedger()
	start := time.Now()
	// Several seeds cover both the early-drop and the read-to-terminal
	// (+ Last-Event-ID resume) paths.
	for seed := uint64(1); seed <= 6; seed++ {
		require.NoError(t, sse.Run(context.Background(), newRng(seed), ledger), "seed %d", seed)
	}
	require.Less(t, time.Since(start), 10*time.Second,
		"sse scenario must finish at terminal/first event, not wait for server close")
	assert.Empty(t, ledger.Verify(context.Background(), stubProbes("paid")))
}

func TestOrderStatusProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "known"):
			writeJSON(w, http.StatusOK, map[string]string{"order_id": "known", "status": "paid"})
		default:
			w.Header().Set("Content-Type", "application/problem+json")
			writeJSON(w, http.StatusNotFound, map[string]any{"status": 404, "code": "GATEWAY_ORDER_NOT_FOUND"})
		}
	}))
	defer srv.Close()

	probe := OrderStatusProbe(srv.URL, srv.Client(), "")
	status, err := probe(context.Background(), "known")
	require.NoError(t, err)
	assert.Equal(t, "paid", status)

	status, err = probe(context.Background(), "missing")
	require.NoError(t, err, "404 means not-yet-visible, not an error")
	assert.Empty(t, status)
}
