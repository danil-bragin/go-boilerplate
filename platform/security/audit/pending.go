package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// InsertPending stages an audit intent for Durable mode: one cheap INSERT into
// audit_pending on the AMBIENT transaction (the command's tx), committing
// atomically with the command and never touching the chain-head lock. chain_id
// is resolved now; created_at is the original event time (µs-truncated — the
// value hashed when the drainer applies it). DrainPending later applies these.
func (s *PgStore) InsertPending(ctx context.Context, e Entry) error {
	metaJSON, err := marshalMetadata(e.Metadata)
	if err != nil {
		return fmt.Errorf("audit: marshal metadata: %w", err)
	}
	at := e.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.Truncate(time.Microsecond)
	db := pg.FromContext(ctx, s.pool)
	if _, err := db.Exec(ctx,
		`insert into audit_pending (chain_id, actor, action, subject, metadata, created_at)
		 values ($1,$2,$3,$4,$5,$6)`,
		s.chainIDFor(e.Actor), e.Actor, e.Action, e.Subject, metaJSON, at); err != nil {
		return fmt.Errorf("audit: stage pending: %w", err)
	}
	return nil
}

type pendingRow struct {
	id    int64
	entry Entry
}

// PendingBacklog returns the number of rows currently staged in audit_pending
// (i.e. intents that have not yet been drained to the hash chain). It uses the
// reader pool so it never contends with the drain writer path. The T5 drain
// worker calls this each tick and passes the result to RecordPendingBacklog.
func (s *PgStore) PendingBacklog(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.Reader().QueryRow(ctx,
		`select count(*) from audit_pending`).Scan(&n); err != nil {
		return 0, fmt.Errorf("audit: pending backlog count: %w", err)
	}
	return n, nil
}

// DrainPending applies up to batchSize staged intents to the hash chain
// exactly-once and returns the number applied (0 = empty). It reads candidate
// ids ordered by (chain_id, id), then for each chain CLAIMS-and-applies them in
// one transaction (see applyClaimed).
//
// Concurrency safety: pg.RunAsLeader is leader ELECTION, not fencing — it
// permits a brief window where an old and a freshly-elected leader both run the
// drain. applyClaimed makes that safe: the claim is a DELETE ... RETURNING, so
// only the ONE transaction whose DELETE actually removes a row applies it; an
// overlapping drainer's DELETE returns that row as already-gone and applies
// nothing. Without this guard two overlapping drainers would double-apply
// intents (validly hash-linked duplicates that VerifyChain would not catch).
func (s *PgStore) DrainPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 128
	}
	rows, err := s.pool.Reader().Query(ctx,
		`select id, chain_id from audit_pending order by chain_id, id limit $1`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("audit: read pending: %w", err)
	}
	byChain := map[int16][]int64{}
	var order []int16
	for rows.Next() {
		var (
			id      int64
			chainID int16
		)
		if err := rows.Scan(&id, &chainID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("audit: scan pending: %w", err)
		}
		if _, seen := byChain[chainID]; !seen {
			order = append(order, chainID)
		}
		byChain[chainID] = append(byChain[chainID], id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("audit: iterate pending: %w", err)
	}

	applied := 0
	for _, chainID := range order {
		n, err := s.applyClaimed(ctx, chainID, byChain[chainID])
		if err != nil {
			return applied, fmt.Errorf("audit: drain chain %d: %w", chainID, err)
		}
		applied += n
	}
	return applied, nil
}

// applyClaimed atomically claims the given audit_pending ids and applies the
// claimed rows to chainID's hash chain, in one transaction. The claim is a
// DELETE ... RETURNING: only rows this transaction actually removes are applied,
// so two transiently-overlapping leaders (RunAsLeader is election, not fencing)
// can never double-apply — the loser's DELETE finds the rows already gone and
// applies nothing. Claimed rows are re-sorted by id (RETURNING order is
// unspecified) to preserve per-chain hash order. Returns the count applied.
func (s *PgStore) applyClaimed(ctx context.Context, chainID int16, ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var applied int
	err := pg.RunInTx(ctx, s.pool, func(ctx context.Context) error {
		db := pg.FromContext(ctx, s.pool)
		rows, err := db.Query(ctx,
			`delete from audit_pending where id = any($1)
			 returning id, actor, action, subject, metadata, created_at`, ids)
		if err != nil {
			return err
		}
		var claimed []pendingRow
		for rows.Next() {
			var (
				id   int64
				e    Entry
				meta []byte
				at   time.Time
			)
			if err := rows.Scan(&id, &e.Actor, &e.Action, &e.Subject, &meta, &at); err != nil {
				rows.Close()
				return err
			}
			e.At = at
			if len(meta) > 0 {
				var m map[string]string
				if err := json.Unmarshal(meta, &m); err != nil {
					rows.Close()
					return err
				}
				e.Metadata = m
			}
			claimed = append(claimed, pendingRow{id: id, entry: e})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(claimed) == 0 {
			return nil // another drainer already claimed these — apply nothing
		}
		sort.Slice(claimed, func(i, j int) bool { return claimed[i].id < claimed[j].id })
		entries := make([]Entry, len(claimed))
		for i, r := range claimed {
			entries[i] = r.entry
		}
		if err := s.RecordBatchSameChain(ctx, chainID, entries); err != nil {
			return err
		}
		applied = len(claimed)
		return nil
	})
	return applied, err
}
