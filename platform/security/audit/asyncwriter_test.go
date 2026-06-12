package audit_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

func TestRecordBatch_SameChain_VerifiesAndMatchesIndividual(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool)

	entries := []audit.Entry{
		{Actor: "u1", Action: "view", Subject: "p1"},
		{Actor: "u1", Action: "view", Subject: "p2"},
		{Actor: "u1", Action: "view", Subject: "p3"},
	}
	cid := store.ChainIDFor("u1")
	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return store.RecordBatchSameChain(ctx, cid, entries)
	}))

	var n int
	require.NoError(t, pool.Reader().QueryRow(ctx, `select count(*) from audit_log where chain_id=$1`, cid).Scan(&n))
	require.Equal(t, 3, n)

	res, err := store.VerifyChain(ctx, time.Time{})
	require.NoError(t, err)
	require.True(t, res.OK, "batched chain write must verify; break id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, 3, res.Verified)
}

func TestBufferedAuditWriter_DrainsBatchedAndVerifies(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := audit.NewPgStore(pool, audit.WithChainShards(4))
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{Buffer: 1024, BatchSize: 64, FlushInterval: 20 * time.Millisecond})
	go func() { _ = w.Run(ctx) }()

	const n = 500
	for i := range n {
		require.True(t, w.Enqueue(audit.Entry{Actor: fmt.Sprintf("u%d", i%8), Action: "view", Subject: fmt.Sprintf("p%d", i)}))
	}
	require.Eventually(t, func() bool {
		var c int
		_ = pool.Reader().QueryRow(ctx, `select count(*) from audit_log`).Scan(&c)
		return c == n
	}, 10*time.Second, 50*time.Millisecond)

	res, err := store.VerifyChain(ctx, time.Time{})
	require.NoError(t, err)
	require.True(t, res.OK, "async-written chain must verify; break id=%d reason=%q", res.BreakID, res.Reason)
}

func TestBufferedAuditWriter_DropsOnOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	// Tiny buffer, NO drain running ⇒ fills, then Enqueue returns false.
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{Buffer: 2, BatchSize: 1, FlushInterval: time.Hour})
	require.True(t, w.Enqueue(audit.Entry{Actor: "u", Action: "v", Subject: "p"}))
	require.True(t, w.Enqueue(audit.Entry{Actor: "u", Action: "v", Subject: "p"}))
	require.False(t, w.Enqueue(audit.Entry{Actor: "u", Action: "v", Subject: "p"}), "full buffer ⇒ drop")
	require.Equal(t, int64(1), w.Dropped())
}

func TestAsyncAudit_EnqueuesAfterHandler_NeverFailsCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{Buffer: 16, FlushInterval: time.Hour})
	b := audit.AsyncAudit[string, string](w, "view", func(c string) string { return c })

	called := false
	h := b(func(_ context.Context, _ string) (string, error) { called = true; return "ok", nil })
	out, err := h(context.Background(), "p1")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
	require.True(t, called)
	require.Equal(t, 1, w.Len(), "entry enqueued after a successful handler")
}

func TestAsyncAudit_HandlerError_DoesNotEnqueue(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	w := audit.NewBufferedAuditWriter(store, audit.WriterConfig{Buffer: 16, FlushInterval: time.Hour})
	b := audit.AsyncAudit[string, string](w, "view", func(c string) string { return c })
	wantErr := errors.New("boom")
	h := b(func(_ context.Context, _ string) (string, error) { return "", wantErr })
	_, err := h(context.Background(), "p1")
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 0, w.Len(), "no audit on handler failure")
}
