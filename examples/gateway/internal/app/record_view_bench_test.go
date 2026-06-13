package app_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/audit"

	"github.com/stretchr/testify/require"
)

// runViewLoad fires `total` RecordProductView commands through h at `conc`
// concurrency, each with a distinct product id, and returns the sustained
// command rate (ops/s). It measures the user-facing command throughput — the
// time the caller waits — NOT the downstream audit-drain time.
func runViewLoad(tb testing.TB, h cqrs.HandlerFunc[gatewayapp.RecordProductView, struct{}], total, conc int) float64 {
	tb.Helper()
	ctx := context.Background()
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range total {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			_, _ = h(ctx, gatewayapp.RecordProductView{
				ProductID: fmt.Sprintf("p-%d", i),
				Viewer:    "shopper", // single actor → Strong audit collapses to one chain
			})
		}(i)
	}
	wg.Wait()
	return float64(total) / time.Since(start).Seconds()
}

// TestRecordProductView_ThroughputContrast is the A2 throughput-contrast
// benchmark (gated by A2_THROUGHPUT=1, needs Docker). It runs the SAME demo
// command twice:
//
//   - Eventual (the shipped wiring): Transactional(false) + async best-effort
//     audit — the command returns after the single insert + a non-blocking
//     Enqueue; the audit chain is written off the hot path, batched per chain.
//   - Strong (forced for contrast): WithConsistency(Strong, pool) wraps the same
//     handler in a transaction AND records audit synchronously inside it
//     (audit.Audit, can't-audit→don't-commit).
//
// With a single actor the Strong path serializes every command on the
// audit_chain_head FOR UPDATE lock (the measured order-create ceiling); the
// Eventual path decouples the command rate from that lock. The contrast is
// LOGGED (the absolute numbers are machine-dependent); the test only asserts the
// Eventual command rate is not lower than Strong's — i.e. relaxing observation
// never costs command throughput.
func TestRecordProductView_ThroughputContrast(t *testing.T) {
	if os.Getenv("A2_THROUGHPUT") != "1" {
		t.Skip("set A2_THROUGHPUT=1 to run the A2 throughput-contrast benchmark (needs Docker)")
	}

	const (
		total = 3000
		conc  = 16
	)

	// --- Eventual: async best-effort audit, no tx wrap (the shipped demo) ---
	poolE := gatewayPool(t)
	ctxE, cancelE := context.WithCancel(context.Background())
	defer cancelE()
	storeE := audit.NewPgStore(poolE)
	w := audit.NewBufferedAuditWriter(storeE, audit.WriterConfig{
		Buffer:        total + 1024, // size so a clean run does not drop
		BatchSize:     128,
		FlushInterval: 20 * time.Millisecond,
	})
	go func() { _ = w.Run(ctxE) }()
	eventualH := gatewayapp.DecorateRecordProductView(gatewayapp.RecordProductViewHandler(poolE), w)
	eventualRate := runViewLoad(t, eventualH, total, conc)

	// Let the async writer drain so we can report how much audit actually landed.
	require.Eventually(t, func() bool {
		var n int
		_ = poolE.Reader().QueryRow(ctxE,
			`select count(*) from audit_log where action = 'product:view'`).Scan(&n)
		return int64(n)+w.Dropped() >= int64(total)
	}, 30*time.Second, 100*time.Millisecond, "async audit should drain (or account drops)")
	var drained int
	require.NoError(t, poolE.Reader().QueryRow(ctxE,
		`select count(*) from audit_log where action = 'product:view'`).Scan(&drained))

	// --- Strong: sync audit inside a transaction (forced for contrast) ---
	poolS := gatewayPool(t)
	storeS := audit.NewPgStore(poolS)
	strongH := cqrs.StandardPipeline[gatewayapp.RecordProductView, struct{}]("RecordProductViewStrong").
		WithConsistency(cqrs.Strong, poolS).
		Use(audit.Audit[gatewayapp.RecordProductView, struct{}](storeS, "product:view",
			func(c gatewayapp.RecordProductView) string { return c.ProductID })).
		Decorate(gatewayapp.RecordProductViewHandler(poolS))
	strongRate := runViewLoad(t, strongH, total, conc)

	t.Logf("A2 throughput contrast — %d commands, concurrency %d, single actor", total, conc)
	t.Logf("  Eventual (async audit, no tx) → %8.0f cmd/s   (audit drained %d, dropped %d)", eventualRate, drained, w.Dropped())
	t.Logf("  Strong   (sync audit in tx)   → %8.0f cmd/s   (every command serializes on the chain-head lock)", strongRate)
	t.Logf("  speedup: %.2fx", eventualRate/strongRate)

	require.GreaterOrEqual(t, eventualRate, strongRate,
		"relaxing observation (Eventual) must not cost command throughput vs Strong")
}
