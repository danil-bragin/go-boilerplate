package audit

import (
	"context"
	"encoding/json"
	"fmt"
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

// DrainPending applies up to batchSize staged intents to the hash chain and
// deletes them, exactly-once: reads pending ordered by (chain_id, id), groups by
// chain, and for each chain group runs RecordBatchSameChain + DELETE in ONE
// transaction. Returns the number applied (0 = empty). MUST be called
// single-active per shard (the chain is applied strictly in id order per chain).
func (s *PgStore) DrainPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 128
	}
	rows, err := s.pool.Reader().Query(ctx,
		`select id, chain_id, actor, action, subject, metadata, created_at
		   from audit_pending order by chain_id, id limit $1`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("audit: read pending: %w", err)
	}
	byChain := map[int16][]pendingRow{}
	var order []int16
	for rows.Next() {
		var (
			id      int64
			chainID int16
			e       Entry
			meta    []byte
			at      time.Time
		)
		if err := rows.Scan(&id, &chainID, &e.Actor, &e.Action, &e.Subject, &meta, &at); err != nil {
			rows.Close()
			return 0, fmt.Errorf("audit: scan pending: %w", err)
		}
		e.At = at
		if len(meta) > 0 {
			var m map[string]string
			if err := json.Unmarshal(meta, &m); err != nil {
				rows.Close()
				return 0, fmt.Errorf("audit: unmarshal pending metadata: %w", err)
			}
			e.Metadata = m
		}
		if _, seen := byChain[chainID]; !seen {
			order = append(order, chainID)
		}
		byChain[chainID] = append(byChain[chainID], pendingRow{id: id, entry: e})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("audit: iterate pending: %w", err)
	}

	applied := 0
	for _, chainID := range order {
		group := byChain[chainID]
		entries := make([]Entry, len(group))
		ids := make([]int64, len(group))
		for i, r := range group {
			entries[i] = r.entry
			ids[i] = r.id
		}
		if err := pg.RunInTx(ctx, s.pool, func(ctx context.Context) error {
			if err := s.RecordBatchSameChain(ctx, chainID, entries); err != nil {
				return err
			}
			_, derr := pg.FromContext(ctx, s.pool).Exec(ctx, `delete from audit_pending where id = any($1)`, ids)
			return derr
		}); err != nil {
			return applied, fmt.Errorf("audit: drain chain %d: %w", chainID, err)
		}
		applied += len(group)
	}
	return applied, nil
}
