// Package audit provides an audit-log store and a CQRS behavior that
// records successful command executions atomically with the command's own
// database transaction.
//
// # Pipeline ordering
//
// The Audit behavior MUST be placed INSIDE the Transaction behavior in the
// pipeline so that the audit INSERT runs within the command's transaction:
//
//	cqrs.Decorate(handler,
//	    cqrs.Transaction[C, R](pool), // outermost — opens the tx
//	    audit.Audit[C, R](store, action, subjectFn), // inner — uses the tx
//	)
//
// If Audit is placed outside Transaction the audit row will be written in a
// separate connection and will NOT roll back when the command tx rolls back.
//
// # Failure policy
//
// Only successful commands are audited. When the handler returns an error the
// audit entry is NOT written (the failure is the caller's concern). However,
// if Record itself returns an error the whole operation is aborted: the error
// is returned to the caller and the surrounding transaction (if any) is rolled
// back. This guarantees that a command whose audit trail cannot be persisted
// never commits its business writes.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"go-boilerplate/platform/security/audit/gen"
	"go-boilerplate/platform/storage/pg"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Entry is a single audit-log record.
type Entry struct {
	Actor    string
	Action   string
	Subject  string
	Metadata map[string]string
	At       time.Time
}

// Store persists audit entries.
type Store interface {
	Record(ctx context.Context, e Entry) error
}

// PgStore writes audit entries to the audit_log table via pg.FromContext so
// that the INSERT participates in the ambient command transaction (if any).
type PgStore struct {
	pool      *pg.Pool
	onError   func(error)
	adminPool *pgxpool.Pool // privileged DELETE pool for retention (SetAdminPool)
}

// NewPgStore returns a PgStore backed by pool.
func NewPgStore(pool *pg.Pool) *PgStore {
	return &PgStore{pool: pool}
}

// SetOnError registers a callback invoked for each Cleanup error inside
// RunCleanup. The loop continues regardless of individual cleanup errors.
func (s *PgStore) SetOnError(fn func(error)) {
	s.onError = fn
}

// Record inserts an audit entry into the GLOBAL hash chain. It uses
// pg.FromContext so the write joins any transaction active on ctx (set by
// pg.RunInTx / cqrs.Transaction).
//
// Chaining: the single audit_chain_head row is locked FOR UPDATE, its
// last_hash becomes the new row's prev_hash, entry_hash =
// sha256(prev_hash || canonical(entry)) is computed, the row is inserted, and
// the head is advanced to entry_hash — all under the lock and inside the
// command transaction. Holding the lock until the command commits serializes
// the chain across concurrent writers: the order is total and gap-free, so
// VerifyChain can recompute it deterministically. The cost is that audit
// writes are serialized globally — see BenchmarkRecord for the contention
// ceiling this imposes (the documented trade-off of a single global chain;
// per-actor chains are the escape hatch when this becomes the bottleneck).
func (s *PgStore) Record(ctx context.Context, e Entry) error {
	metaJSON, err := marshalMetadata(e.Metadata)
	if err != nil {
		return fmt.Errorf("audit: marshal metadata: %w", err)
	}
	db := pg.FromContext(ctx, s.pool)

	// Lock + read the chain head. FOR UPDATE serializes concurrent Records:
	// the next writer blocks here until this command's tx commits/rolls back.
	var prevHash []byte
	if err := db.QueryRow(
		ctx,
		`select last_hash from audit_chain_head where id = 1 for update`,
	).Scan(&prevHash); err != nil {
		return fmt.Errorf("audit: lock chain head: %w", err)
	}

	at := e.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	entryHash := computeEntryHash(prevHash, e.Actor, e.Action, e.Subject, at, e.Metadata)

	if _, err := db.Exec(
		ctx,
		`insert into audit_log (actor, action, subject, metadata, created_at, prev_hash, entry_hash)
		 values ($1, $2, $3, $4, $5, $6, $7)`,
		e.Actor, e.Action, e.Subject, metaJSON, at, prevHash, entryHash,
	); err != nil {
		return fmt.Errorf("audit: record: %w", err)
	}

	if _, err := db.Exec(
		ctx,
		`update audit_chain_head set last_hash = $1, updated_at = now() where id = 1`,
		entryHash,
	); err != nil {
		return fmt.Errorf("audit: advance chain head: %w", err)
	}
	return nil
}

// computeEntryHash returns sha256(prevHash || canonical(entry)). The canonical
// serialization is deterministic and unambiguous: every field is
// length-prefixed (8-byte big-endian length + bytes) so no concatenation of
// adjacent fields can be confused with a different split, and metadata keys are
// emitted in sorted order. The occurred-at timestamp is encoded as UTC
// UnixNano so a row's hash does not depend on the connection time zone.
func computeEntryHash(prevHash []byte, actor, action, subject string, at time.Time, metadata map[string]string) []byte {
	h := sha256.New()
	h.Write(prevHash)
	writeField(h, []byte(actor))
	writeField(h, []byte(action))
	writeField(h, []byte(subject))
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(at.UTC().UnixNano())) //nolint:gosec // wall-clock ns fits int64 for any realistic timestamp; the cast is a fixed-width encoding
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
		writeField(h, []byte(k))
		writeField(h, []byte(metadata[k]))
	}
	return h.Sum(nil)
}

// writeField writes an 8-byte big-endian length prefix followed by b, so the
// concatenation of fields is unambiguous (no field boundary can be forged by
// shifting bytes between adjacent fields).
func writeField(h interface{ Write([]byte) (int, error) }, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

// ChainResult is the outcome of a VerifyChain walk.
type ChainResult struct {
	// OK is true when every row's stored entry_hash matches the recomputed
	// hash AND each row's prev_hash links to the previous row's entry_hash.
	OK bool
	// Verified is the number of rows walked.
	Verified int
	// BreakID is the id of the first row whose hash failed to verify, or 0
	// when OK. ExpectedHash/GotHash are that row's recomputed vs stored
	// entry_hash (hex-encoded) for forensic reporting.
	BreakID      int64
	ExpectedHash string
	GotHash      string
	// Reason describes the break ("entry_hash mismatch" or "prev_hash link
	// broken"); empty when OK.
	Reason string
}

// VerifyChain walks the audit chain in insertion order (id ascending), starting
// at the first row with created_at >= since (pass the zero time to walk the
// whole table), recomputing each entry_hash and checking the prev_hash links.
// It returns the first break it finds, or OK=true when the walk is clean.
//
// Tamper detection: an attacker who edits a historical row in place (e.g. a
// superuser UPDATE that bypasses the append-only REVOKE) changes that row's
// recomputed entry_hash, so VerifyChain flags it at its id. Because each
// row's prev_hash also pins the previous row's entry_hash, deleting or
// reordering rows is detected too.
//
// NOTE: when since is non-zero the walk starts mid-chain, so prev_hash linkage
// is checked from the first walked row's stored prev_hash forward — a tamper
// strictly before `since` that does not touch any walked row is out of scope
// for that partial walk (use the zero time for a full audit).
func (s *PgStore) VerifyChain(ctx context.Context, since time.Time) (ChainResult, error) {
	rows, err := pg.FromContextRead(ctx, s.pool).Query(
		ctx,
		`select id, actor, action, subject, metadata, created_at, prev_hash, entry_hash
		 from audit_log
		 where created_at >= $1
		 order by id asc`,
		since.UTC(),
	)
	if err != nil {
		return ChainResult{}, fmt.Errorf("audit: verify chain: %w", err)
	}
	defer rows.Close()

	var (
		res      ChainResult
		expPrev  []byte
		havePrev bool
	)
	for rows.Next() {
		var (
			id        int64
			actor     string
			action    string
			subject   string
			metaRaw   []byte
			createdAt pgtype.Timestamptz
			prevHash  []byte
			entryHash []byte
		)
		if err := rows.Scan(&id, &actor, &action, &subject, &metaRaw, &createdAt, &prevHash, &entryHash); err != nil {
			return ChainResult{}, fmt.Errorf("audit: verify chain scan: %w", err)
		}

		// Link check: this row's prev_hash must equal the previous walked
		// row's entry_hash (skipped for the first walked row).
		if havePrev && !bytesEqual(prevHash, expPrev) {
			res.BreakID = id
			res.Reason = "prev_hash link broken"
			res.ExpectedHash = hexOf(expPrev)
			res.GotHash = hexOf(prevHash)
			return res, nil
		}

		var meta map[string]string
		if len(metaRaw) > 0 {
			_ = json.Unmarshal(metaRaw, &meta)
		}
		want := computeEntryHash(prevHash, actor, action, subject, createdAt.Time, meta)
		if !bytesEqual(want, entryHash) {
			res.BreakID = id
			res.Reason = "entry_hash mismatch"
			res.ExpectedHash = hexOf(want)
			res.GotHash = hexOf(entryHash)
			return res, nil
		}

		res.Verified++
		expPrev = entryHash
		havePrev = true
	}
	if err := rows.Err(); err != nil {
		return ChainResult{}, fmt.Errorf("audit: verify chain rows: %w", err)
	}
	res.OK = true
	return res, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hexOf(b []byte) string { return fmt.Sprintf("%x", b) }

// Query returns actor's audit entries whose created_at >= since (INCLUSIVE;
// pass the zero time for "everything"), newest first, capped at limit rows.
// limit <= 0 falls back to a defensive default of 100.
//
// This is the DSAR/audit read path (data subject access requests, incident
// forensics): reads go through pg.FromContextRead, so they hit the reader
// pool (or join an ambient transaction) and never queue on the writer.
func (s *PgStore) Query(ctx context.Context, actor string, since time.Time, limit int) ([]Entry, error) {
	if limit <= 0 {
		limit = 100
	}
	rowLimit := int32(math.MaxInt32)
	if limit < math.MaxInt32 {
		rowLimit = int32(limit)
	}
	rows, err := gen.New(pg.FromContextRead(ctx, s.pool)).QueryByActor(ctx, gen.QueryByActorParams{
		Actor:    actor,
		Since:    pgtype.Timestamptz{Time: since, Valid: true},
		RowLimit: rowLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("audit: query by actor: %w", err)
	}
	out := make([]Entry, len(rows))
	for i, r := range rows {
		var meta map[string]string
		if len(r.Metadata) > 0 {
			if err := json.Unmarshal(r.Metadata, &meta); err != nil {
				// The column is open jsonb: a single row written by another
				// tool (manual forensics insert, future schema user) must not
				// 500 the whole DSAR query — surface the raw payload instead.
				meta = map[string]string{"_raw": string(r.Metadata)}
			}
		}
		out[i] = Entry{
			Actor:    r.Actor,
			Action:   r.Action,
			Subject:  r.Subject,
			Metadata: meta,
			At:       r.CreatedAt.Time,
		}
	}
	return out, nil
}

// marshalMetadata converts a string map (possibly nil) to a JSON byte slice.
// A nil map produces the JSON literal "{}".
func marshalMetadata(m map[string]string) ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}
