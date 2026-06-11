package audit_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"go-boilerplate/platform/config"
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
				Metadata: map[string]string{"seq": strconv.Itoa(i), "ip": "10.0.0.1"},
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

// TestVerifyChain_RoundTripsThroughDB is the regression guard for the
// nanosecond-vs-microsecond truncation bug: Record hashes the timestamp as
// UnixNano, but timestamptz stores only microseconds. If Record hashed a
// full-nanosecond `at` while the column truncated it, VerifyChain — which
// recomputes from the µs-truncated created_at it reads BACK FROM THE DB — would
// mismatch on every clean row. We deliberately feed an entry whose At carries
// sub-microsecond nanoseconds; the round trip must still verify, proving Record
// truncates to µs before BOTH hashing and INSERT.
func TestVerifyChain_RoundTripsThroughDB(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()

	// 123456789ns has sub-µs digits (…789) that timestamptz cannot store.
	at := time.Date(2026, 6, 12, 10, 30, 0, 123456789, time.UTC)
	for i := range 5 {
		e := audit.Entry{
			Actor:    "rt-actor",
			Action:   "order:create",
			Subject:  fmt.Sprintf("order-%d", i),
			Metadata: map[string]string{"seq": strconv.Itoa(i)},
			At:       at.Add(time.Duration(i) * time.Second),
		}
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return store.Record(ctx, e)
		}))
	}

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK,
		"sub-µs timestamp round-tripped through timestamptz must still verify; break at id=%d reason=%q",
		res.BreakID, res.Reason)
	require.Equal(t, 5, res.Verified)
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

// chainKey is a fixed HMAC key for the keyed-chain tests.
const chainKey = "test-audit-chain-hmac-key-32bytes!!"

// keylessEntryHash recomputes the canonical KEYLESS sha256 entry hash exactly
// as production computeEntryHash does with an empty key. It models a forger who
// holds an app (INSERT) connection but NOT the HMAC key: the best they can do
// is compute a plain-sha256 hash, which a keyed VerifyChain will reject.
func keylessEntryHash(prevHash []byte, actor, action, subject string, at time.Time, metadata map[string]string) []byte {
	h := sha256.New()
	h.Write(prevHash)
	writeLP(h, []byte(actor))
	writeLP(h, []byte(action))
	writeLP(h, []byte(subject))
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(at.UTC().UnixNano()))
	h.Write(ts[:])
	keys := make([]string, 0, len(metadata))
	for k := range metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var cnt [8]byte
	binary.BigEndian.PutUint64(cnt[:], uint64(len(keys)))
	h.Write(cnt[:])
	for _, k := range keys {
		writeLP(h, []byte(k))
		writeLP(h, []byte(metadata[k]))
	}
	return h.Sum(nil)
}

func writeLP(h interface{ Write([]byte) (int, error) }, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

// TestVerifyChain_KeyedVerifies: a chain built AND verified with the same HMAC
// key round-trips clean.
func TestVerifyChain_KeyedVerifies(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool, audit.WithChainKey(config.Secret(chainKey)))
	recordN(t, pool, store, 15)

	res, err := store.VerifyChain(context.Background(), zeroTime())
	require.NoError(t, err)
	require.True(t, res.OK, "keyed chain must verify; break at id=%d reason=%q", res.BreakID, res.Reason)
	require.Equal(t, 15, res.Verified)
}

// TestVerifyChain_KeyedDetectsForgedAppend: an attacker with an app (INSERT)
// connection but NO HMAC key forges a NEW trailing row using keyless sha256 and
// advances the head to match. Against a KEYED VerifyChain the forged row's
// entry_hash does not equal the HMAC the verifier recomputes, so the forgery is
// detected — the forgery-resistance the key buys over keyless sha256.
func TestVerifyChain_KeyedDetectsForgedAppend(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool, audit.WithChainKey(config.Secret(chainKey)))
	ctx := context.Background()
	recordN(t, pool, store, 8)

	// Read the current head — the forger appends after it.
	var head []byte
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select last_hash from audit_chain_head where id = 1`).Scan(&head))

	at := time.Now().UTC().Truncate(time.Microsecond)
	forgedMeta := map[string]string{"forged": "1"}
	forgedHash := keylessEntryHash(head, "attacker", "order:create", "evil-order", at, forgedMeta)

	// Append the forged row + advance the head (what an INSERT-capable forger
	// without the key can do). prev_hash links cleanly; only the keyed hash math
	// betrays it.
	_, err := pool.Writer().Exec(ctx,
		`insert into audit_log (actor, action, subject, metadata, created_at, prev_hash, entry_hash)
		 values ('attacker','order:create','evil-order', '{"forged":"1"}'::jsonb, $1, $2, $3)`,
		at, head, forgedHash)
	require.NoError(t, err)
	_, err = pool.Writer().Exec(ctx,
		`update audit_chain_head set last_hash = $1 where id = 1`, forgedHash)
	require.NoError(t, err)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "keyed verify must detect a keyless forged append")
	require.Equal(t, "entry_hash mismatch", res.Reason)
}

// TestVerifyChain_DetectsHeadTruncation: deleting the most recent row leaves a
// self-consistent prefix walk, but the recorded chain head still points at the
// deleted tip — VerifyChain's head check catches it.
func TestVerifyChain_DetectsHeadTruncation(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	recordN(t, pool, store, 10)

	var lastID int64
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select id from audit_log order by id desc limit 1`).Scan(&lastID))
	_, err := pool.Writer().Exec(ctx, `delete from audit_log where id = $1`, lastID)
	require.NoError(t, err)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "deleting the head row must NOT verify")
	require.Equal(t, "chain head mismatch", res.Reason)
}

// TestVerifyChain_DetectsGenesisTruncation: deleting the FIRST (genesis) row
// re-anchors the surviving first row to a non-zero prev_hash — the genesis
// anchor check catches it.
func TestVerifyChain_DetectsGenesisTruncation(t *testing.T) {
	pool := newPool(t)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	recordN(t, pool, store, 10)

	var firstID int64
	require.NoError(t, pool.Writer().QueryRow(ctx,
		`select id from audit_log order by id asc limit 1`).Scan(&firstID))
	_, err := pool.Writer().Exec(ctx, `delete from audit_log where id = $1`, firstID)
	require.NoError(t, err)

	res, err := store.VerifyChain(ctx, zeroTime())
	require.NoError(t, err)
	require.False(t, res.OK, "deleting the genesis row must NOT verify")
	require.Equal(t, "genesis anchor mismatch", res.Reason)
}
