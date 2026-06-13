package audit_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

// pendingCount returns the number of rows currently staged in audit_pending.
func pendingCount(t *testing.T, pool *pg.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select count(*) from audit_pending`).Scan(&n))
	return n
}

// auditLogCount returns the number of rows in the append-only audit_log chain.
func auditLogCount(t *testing.T, pool *pg.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select count(*) from audit_log`).Scan(&n))
	return n
}

// TestInsertPending_StagesWithoutTouchingChain verifies the Durable command-time
// path: InsertPending stages exactly one intent into audit_pending on the
// ambient tx and leaves the hash chain (audit_log) completely untouched — no
// chain-head lock, no chain row at command time.
func TestInsertPending_StagesWithoutTouchingChain(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool)

	require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return store.InsertPending(ctx, audit.Entry{
			Actor:    "alice",
			Action:   "order.place",
			Subject:  "order-1",
			Metadata: map[string]string{"k": "v"},
		})
	}))

	require.Equal(t, 1, pendingCount(t, pool), "intent staged in audit_pending")
	require.Equal(t, 0, auditLogCount(t, pool), "chain untouched at command time")
}

// TestDrainPending_AppliesExactlyOnceAndVerifies stages 200 intents across 8
// actors (sharded into 4 chains), drains them to the hash chain, and asserts:
// every intent is applied exactly once, audit_pending is emptied, the chain
// verifies, and a second drain is a no-op (exactly-once).
func TestDrainPending_AppliesExactlyOnceAndVerifies(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := audit.NewPgStore(pool, audit.WithChainShards(4))

	const n = 200
	for i := 0; i < n; i++ {
		actor := fmt.Sprintf("u%d", i%8)
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return store.InsertPending(ctx, audit.Entry{
				Actor:    actor,
				Action:   "order.place",
				Subject:  fmt.Sprintf("order-%d", i),
				Metadata: map[string]string{"seq": strconv.Itoa(i)},
				At:       time.Now().UTC().Add(time.Duration(i) * time.Microsecond),
			})
		}))
	}
	require.Equal(t, n, pendingCount(t, pool), "all intents staged")
	require.Equal(t, 0, auditLogCount(t, pool), "chain still untouched before drain")

	total := 0
	for {
		applied, err := store.DrainPending(ctx, 32)
		require.NoError(t, err)
		if applied == 0 {
			break
		}
		total += applied
	}
	require.Equal(t, n, total, "every staged intent applied exactly once")
	require.Equal(t, 0, pendingCount(t, pool), "pending drained")
	require.Equal(t, n, auditLogCount(t, pool), "every intent on the chain")

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK, "chain verifies after durable drain: %+v", res)
	require.Equal(t, n, res.Verified, "all rows walked across chains")

	// Exactly-once: draining an empty table applies nothing.
	again, err := store.DrainPending(ctx, 32)
	require.NoError(t, err)
	require.Equal(t, 0, again, "second drain is a no-op")
}
