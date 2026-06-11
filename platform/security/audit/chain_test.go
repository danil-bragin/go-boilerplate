package audit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// zeroTime is the "walk the whole chain" sentinel for VerifyChain.
func zeroTime() time.Time { return time.Time{} }

// recordN writes n chained audit entries through the store (each in its own
// short tx via pg.RunInTx, mirroring how the Audit behavior records inside the
// command tx).
func recordN(t *testing.T, pool *pg.Pool, store *audit.PgStore, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return store.Record(ctx, audit.Entry{
				Actor:    fmt.Sprintf("actor-%d", i%3),
				Action:   "order:create",
				Subject:  fmt.Sprintf("order-%d", i),
				Metadata: map[string]string{"seq": fmt.Sprintf("%d", i), "ip": "10.0.0.1"},
			})
		})
		require.NoError(t, err)
	}
}

// TestVerifyChain_CleanVerifies: a chain built by Record verifies end-to-end.
func TestVerifyChain_CleanVerifies(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	recordN(t, pool, store, 25)

	res, err := store.VerifyChain(context.Background(), zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK, "clean chain must verify; break at id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, 25, res.Verified)
}

// TestVerifyChain_DetectsTamper: a superuser UPDATE of a historical row's
// payload (bypassing the append-only REVOKE) breaks the chain; VerifyChain
// reports the break at that row's id.
func TestVerifyChain_DetectsTamper(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	recordN(t, pool, store, 10)

	// Find the id of the 5th row and tamper with its subject in place.
	var tamperID int64
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select id from audit_log order by id asc offset 4 limit 1`).Scan(&tamperID))
	_, err := pool.Writer().Exec(ctx,
		`update audit_log set subject = 'tampered' where id = $1`, tamperID)
	require.NoError(t, err, "test tamper UPDATE runs as the owner/superuser")

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "tampered chain must NOT verify")
	require.Equal(t, tamperID, res.BreakID, "break must be reported at the tampered row")
	require.Equal(t, "entry_hash mismatch", res.Reason)
	require.NotEqual(t, res.ExpectedHash, res.GotHash)
}

// TestVerifyChain_DetectsDeletion: deleting a middle row breaks the prev_hash
// link of the row that followed it.
func TestVerifyChain_DetectsDeletion(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	recordN(t, pool, store, 10)

	var delID, nextID int64
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select id from audit_log order by id asc offset 4 limit 1`).Scan(&delID))
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select id from audit_log order by id asc offset 5 limit 1`).Scan(&nextID))
	_, err := pool.Writer().Exec(ctx, `delete from audit_log where id = $1`, delID)
	require.NoError(t, err)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "chain with a deleted row must NOT verify")
	require.Equal(t, nextID, res.BreakID, "break must surface at the row whose prev_hash no longer links")
	require.Equal(t, "prev_hash link broken", res.Reason)
}

// TestVerifyChain_ConcurrentWriters: many concurrent commands recording audit
// entries still produce a totally-ordered, gap-free chain that verifies. The
// FOR UPDATE on the chain head serializes them.
func TestVerifyChain_ConcurrentWriters(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()

	const writers = 16
	const perWriter = 8
	g, gctx := errgroup.WithContext(ctx)
	for w := range writers {
		g.Go(func() error {
			for i := range perWriter {
				if err := pg.RunInTx(gctx, pool, func(ctx context.Context) error {
					return store.Record(ctx, audit.Entry{
						Actor:   fmt.Sprintf("w-%d", w),
						Action:  "concurrent:write",
						Subject: fmt.Sprintf("w%d-%d", w, i),
					})
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK, "concurrent chain must verify; break at id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, writers*perWriter, res.Verified, "every concurrent write must be chained exactly once")
}
