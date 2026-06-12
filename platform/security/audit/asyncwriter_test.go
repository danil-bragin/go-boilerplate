package audit_test

import (
	"context"
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
