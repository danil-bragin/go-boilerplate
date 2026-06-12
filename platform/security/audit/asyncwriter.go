package audit

import (
	"context"
	"sync/atomic"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// WriterConfig tunes the BufferedAuditWriter.
type WriterConfig struct {
	Buffer        int           // bounded channel capacity; Enqueue drops when full
	BatchSize     int           // max entries drained into one flush
	FlushInterval time.Duration // flush even a partial batch this often
}

// BufferedAuditWriter is the async, best-effort audit path for Eventual-
// consistency commands (A2). Enqueue is non-blocking and drops (with a metric)
// when the buffer is full; a background drainer groups entries by chain and
// writes each chain-group in ONE tx via RecordBatchSameChain, amortising the
// chain-head lock. Buffered-but-undrained entries are lost on crash — the
// documented best-effort trade for Eventual flows. Effectively-once and Strong
// flows do NOT use this path.
type BufferedAuditWriter struct {
	store   *PgStore
	cfg     WriterConfig
	ch      chan Entry
	dropped atomic.Int64
	metrics writerMetrics
}

// NewBufferedAuditWriter builds the async writer over store. Zero/negative cfg
// fields fall back to sane defaults (4096 buffer, 128 batch, 100ms flush).
func NewBufferedAuditWriter(store *PgStore, cfg WriterConfig) *BufferedAuditWriter {
	if cfg.Buffer <= 0 {
		cfg.Buffer = 4096
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 128
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	return &BufferedAuditWriter{store: store, cfg: cfg, ch: make(chan Entry, cfg.Buffer), metrics: newWriterMetrics()}
}

// Enqueue offers an entry to the buffer. Non-blocking: returns false and counts
// a drop (atomic counter + metric) when the buffer is full.
func (w *BufferedAuditWriter) Enqueue(e Entry) bool {
	select {
	case w.ch <- e:
		return true
	default:
		w.dropped.Add(1)
		w.metrics.addDropped(context.Background())
		return false
	}
}

// Dropped reports the total number of entries dropped because the buffer was
// full since this writer was created.
func (w *BufferedAuditWriter) Dropped() int64 { return w.dropped.Load() }

// Len reports the current buffer depth (used by the AsyncAudit behavior test).
func (w *BufferedAuditWriter) Len() int { return len(w.ch) }

// Run drains the buffer until ctx is cancelled, then flushes what remains on a
// short detached context. Launch as a goroutine (servicekit.AddAuditWriter).
func (w *BufferedAuditWriter) Run(ctx context.Context) error {
	t := time.NewTicker(w.cfg.FlushInterval)
	defer t.Stop()
	batch := make([]Entry, 0, w.cfg.BatchSize)
	for {
		select {
		case <-ctx.Done():
			w.drainRemaining(batch)
			return ctx.Err()
		case e := <-w.ch:
			batch = append(batch, e)
			if len(batch) >= w.cfg.BatchSize {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-t.C:
			if len(batch) > 0 {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

// flush groups the batch by chain and writes each chain-group in one tx via
// RecordBatchSameChain, amortising the chain-head FOR UPDATE lock across the
// group. Grouping preserves per-chain enqueue order (range over the slice
// preserves order; map grouping preserves per-key append order), so the
// resulting chain segment is deterministic and passes VerifyChain — do NOT
// sort within a group.
func (w *BufferedAuditWriter) flush(ctx context.Context, batch []Entry) {
	if len(batch) == 0 {
		return
	}
	byChain := map[int16][]Entry{}
	for _, e := range batch {
		cid := w.store.ChainIDFor(e.Actor)
		byChain[cid] = append(byChain[cid], e)
	}
	for cid, entries := range byChain {
		err := pg.RunInTx(ctx, w.store.pool, func(ctx context.Context) error {
			return w.store.RecordBatchSameChain(ctx, cid, entries)
		})
		if err != nil {
			w.metrics.addError(ctx)
			if w.store.onError != nil {
				w.store.onError(err)
			}
		}
	}
}

// drainRemaining flushes the in-hand batch plus everything still queued on a
// bounded detached context, so a graceful shutdown does not lose buffered
// entries (the best-effort guarantee for the non-crash path).
func (w *BufferedAuditWriter) drainRemaining(pending []Entry) {
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.flush(dctx, pending)
	for {
		batch := make([]Entry, 0, w.cfg.BatchSize)
		for len(batch) < w.cfg.BatchSize {
			select {
			case e := <-w.ch:
				batch = append(batch, e)
			default:
				w.flush(dctx, batch)
				return
			}
		}
		w.flush(dctx, batch)
	}
}
