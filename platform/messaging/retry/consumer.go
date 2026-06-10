package retry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go-boilerplate/platform/messaging/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartition identifies a unique topic+partition pair.
type topicPartition struct {
	topic     string
	partition int32
}

// wakeEvent carries a topicPartition and the generation tag of the heldBatch
// that scheduled the wake, so drainWakes can discard stale events.
type wakeEvent struct {
	tp  topicPartition
	seq uint64
}

// heldBatch stores records that are not yet due for redelivery.
type heldBatch struct {
	records []*kgo.Record
	dueAt   time.Time
	seq     uint64 // monotonically-increasing generation tag (Fix 4)
}

// escalateRetryDelay is how long a partition is held after a FAILED
// escalation (handler failed AND the escalation/DLT produce also failed,
// e.g. broker unavailable). The held records — starting with the one whose
// escalation failed — are retried in order after this delay. Without the
// hold the record would be skipped permanently: kgo advances its consume
// position on PollFetches regardless of commits, so "do not commit" alone
// does NOT cause redelivery, and committing any later record would advance
// past the failed one (silent loss).
const escalateRetryDelay = time.Second

// Consumer consumes every retry tier of the given base topics and redrives
// records to the original handler once their retry-due-at time arrives.
//
// Not-yet-due records are HELD in memory and their partition is paused so
// no further records are fetched for it (bounding memory to one poll's
// worth per partition). A timer fires at due-time: the held records are
// processed in order, committed, and the partition resumed. Other
// partitions/topics flow uninterrupted — a waiting tier never blocks the
// main topic.
//
// Rebalance-during-hold behaviour: held records' offsets are uncommitted.
// If a rebalance revokes a paused partition, the new owner re-fetches those
// records from the last committed offset. The due-at check in the handler
// means redelivery is idempotent: records are only processed once they are
// truly due. Downstream dedup (inbox table) handles any duplicates.
type Consumer struct {
	client  *kgo.Client
	handler kafka.HandlerFunc
	esc     *Escalator
	policy  Policy
	log     *slog.Logger
	onError func(error)

	// wake receives wakeEvent values when a held batch becomes due.
	// Buffered so AfterFunc goroutines never block (fall through to done-escape).
	wake chan wakeEvent

	// held stores not-yet-due record batches keyed by topicPartition.
	// Only accessed from the single Run goroutine (except onRevoked which
	// is called by kgo callbacks during poll — BlockRebalanceOnPoll
	// guarantees these callbacks run while we are inside PollFetches,
	// i.e. Run is not processing held; mu guards the shared access).
	mu     sync.Mutex
	held   map[topicPartition]*heldBatch
	timers map[topicPartition]*time.Timer

	// done is closed exactly once by Close, signalling all timer callbacks
	// to abandon any pending send on wake.
	done      chan struct{}
	closeOnce sync.Once

	// nextSeq is the per-Consumer source of heldBatch generation tags.
	nextSeq atomic.Uint64
}

// Option configures a Consumer.
type Option func(*Consumer)

// WithOnError sets a callback invoked for non-fatal operational errors
// (e.g. commit failures). Defaults to a no-op.
func WithOnError(fn func(error)) Option {
	return func(c *Consumer) { c.onError = fn }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *Consumer) { c.log = l }
}

// NewConsumer builds a Consumer that subscribes to all retry-tier topics
// derived from baseTopics and the given policy. groupID must be non-empty.
//
// The underlying kgo.Client mirrors the options used by kafka.NewConsumer:
// cooperative-sticky balancer, manual commit, BlockRebalanceOnPoll, and
// ConsumeResetOffset(AtStart).
func NewConsumer(
	cfg kafka.Config,
	groupID string,
	baseTopics []string,
	handler kafka.HandlerFunc,
	esc *Escalator,
	policy Policy,
	opts ...Option,
) (*Consumer, error) {
	if groupID == "" {
		return nil, errors.New("retry: NewConsumer: groupID must not be empty")
	}
	if len(baseTopics) == 0 {
		return nil, errors.New("retry: NewConsumer: at least one base topic is required")
	}
	if handler == nil {
		return nil, errors.New("retry: NewConsumer: handler must not be nil")
	}
	if esc == nil {
		return nil, errors.New("retry: NewConsumer: escalator must not be nil")
	}
	if len(policy.Tiers) == 0 {
		return nil, errors.New("retry: NewConsumer: policy must have at least one tier")
	}

	// Expand base topics → index-named tier topics ("<base>.retry.<idx>").
	tierTopics := make([]string, 0, len(baseTopics)*len(policy.Tiers))
	for _, base := range baseTopics {
		for i := range policy.Tiers {
			tierTopics = append(tierTopics, TierTopic(base, i))
		}
	}

	c := &Consumer{
		handler: handler,
		esc:     esc,
		policy:  policy,
		log:     slog.Default(),
		onError: func(error) {},
		wake:    make(chan wakeEvent, 256),
		held:    make(map[topicPartition]*heldBatch),
		timers:  make(map[topicPartition]*time.Timer),
		done:    make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}

	cl, err := kafka.NewClient(
		cfg,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(tierTopics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Read only committed records to respect EOS producers; no behaviour
		// change on non-transactional retry-tier topics.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Fix 2: clean up held state for revoked/lost partitions so stale timers
		// cannot resume partitions we no longer own.
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onRevoked),
	)
	if err != nil {
		return nil, fmt.Errorf("retry: NewConsumer: %w", err)
	}

	c.client = cl
	return c, nil
}

// onRevoked is called by kgo when partitions are revoked or lost. It removes
// held batches and stops timers for the affected partitions so stale wakes
// cannot resume a partition we no longer own after re-assignment, and it
// REMOVES the partitions from the client's paused set: kgo's paused set
// survives revoke/reassign, so a partition revoked while held (paused) and
// later reassigned to this same instance would otherwise never be fetched
// again — the tier would stall silently forever. Resuming a partition we do
// not own is harmless local bookkeeping.
//
// Concurrency: kgo calls this from inside PollFetches (BlockRebalanceOnPoll).
// The Run loop does NOT hold mu while polling, so there is no deadlock.
func (c *Consumer) onRevoked(_ context.Context, cl *kgo.Client, m map[string][]int32) {
	c.mu.Lock()
	for topic, partitions := range m {
		for _, p := range partitions {
			tp := topicPartition{topic: topic, partition: p}
			delete(c.held, tp)
			if t, ok := c.timers[tp]; ok {
				t.Stop()
				delete(c.timers, tp)
			}
		}
	}
	c.mu.Unlock()

	if cl != nil {
		cl.ResumeFetchPartitions(m)
	}
}

// Run enters the poll-and-redrive loop. It returns when ctx is cancelled or
// the underlying kgo client is closed.
//
// Loop structure (single goroutine — no concurrent commits):
//  1. Drain any wake signals (held batches whose due-time has arrived).
//  2. PollFetches with a short context timeout (~1 s) so wake signals are
//     serviced promptly even when the broker has nothing new.
//  3. For each fetched partition batch: process due records immediately,
//     hold the first not-yet-due record and the rest, pause the partition,
//     schedule a wake timer.
//  4. CommitRecords for all processed records this iteration.
//  5. AllowRebalance.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		// ── 1. Drain wake signals ────────────────────────────────────────────
		var toCommit []*kgo.Record
		toCommit = c.drainWakes(ctx, toCommit)

		// ── 2. PollFetches with short timeout ────────────────────────────────
		pollCtx, pollCancel := context.WithTimeout(ctx, time.Second)
		fetches := c.client.PollFetches(pollCtx)
		pollCancel()

		// Check parent context first.
		if ctx.Err() != nil {
			c.client.AllowRebalance()
			return ctx.Err()
		}

		if fetches.IsClientClosed() {
			c.client.AllowRebalance()
			return nil
		}

		// Handle fetch-level errors.
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) ||
				errors.Is(fe.Err, context.DeadlineExceeded) {
				// The poll context deadline was hit — this is normal; continue.
				continue
			}
			if errors.Is(fe.Err, kgo.ErrClientClosed) {
				c.client.AllowRebalance()
				return nil
			}
			c.onError(fmt.Errorf("retry consumer: fetch error topic=%s partition=%d: %w",
				fe.Topic, fe.Partition, fe.Err))
		}

		// ── 3. Process fetched partitions ────────────────────────────────────
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			toCommit = c.processPartition(ctx, p, toCommit)
		})

		// ── 4. Commit all processed records ─────────────────────────────────
		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, kgo.ErrClientClosed) {
				c.onError(fmt.Errorf("retry consumer: CommitRecords: %w", err))
			}
		}

		// ── 5. Allow next rebalance ─────────────────────────────────────────
		c.client.AllowRebalance()
	}
}

// drainWakes processes any topicPartitions whose held batch is now due.
// Returns the updated toCommit slice.
func (c *Consumer) drainWakes(ctx context.Context, toCommit []*kgo.Record) []*kgo.Record {
	for {
		select {
		case ev := <-c.wake:
			c.mu.Lock()
			batch, ok := c.held[ev.tp]
			// Fix 4: ignore stale wake events — the batch was replaced or removed.
			if !ok || batch.seq != ev.seq {
				c.mu.Unlock()
				continue
			}
			delete(c.held, ev.tp)
			delete(c.timers, ev.tp)
			c.mu.Unlock()

			// Process all held records in order. On a failed escalation,
			// re-hold the failed record and everything after it (the
			// partition stays paused) so nothing is skipped or lost.
			reheld := false
			for j, rec := range batch.records {
				var failed bool
				toCommit, failed = c.handleRecord(ctx, rec, toCommit)
				if failed {
					c.holdRecords(ev.tp, batch.records[j:], time.Now().Add(escalateRetryDelay))
					reheld = true
					break
				}
			}
			if reheld {
				continue
			}

			// Resume fetching this partition.
			c.client.ResumeFetchPartitions(map[string][]int32{ev.tp.topic: {ev.tp.partition}})
		default:
			return toCommit
		}
	}
}

// processPartition processes a single fetched partition batch.
// Records that are past-due are handled immediately; the first not-yet-due
// record (and all following ones) are held and the partition is paused.
func (c *Consumer) processPartition(
	ctx context.Context,
	p kgo.FetchTopicPartition,
	toCommit []*kgo.Record,
) []*kgo.Record {
	now := time.Now()
	tp := topicPartition{topic: p.Topic, partition: p.Partition}

	for i, rec := range p.Records {
		r := recordFromKGO(rec)
		attempt, orig, due, ok := ParseRetryHeaders(r)
		_ = attempt

		if !ok {
			// Malformed record: no retry headers. Derive base topic and escalate to DLT.
			base, baseOK := BaseTopic(rec.Topic)
			if !baseOK {
				base = rec.Topic // fallback: use topic as-is
			}
			origTopic := base
			// Escalate directly to DLT (attempt >= len(tiers) by passing a
			// record with attempt == len(tiers)).
			dltRec := kafka.Record{
				Topic:   DLTTopic(origTopic),
				Key:     rec.Key,
				Value:   rec.Value,
				Headers: headersFromKGO(rec.Headers),
			}
			dltRec.Headers["retry-last-error"] = "malformed: missing retry headers"
			// Fix 3: only commit when DLT produce succeeded. On failure, HOLD
			// this record and the rest of the batch and retry after a delay —
			// merely skipping the commit would NOT redeliver it (kgo advances
			// its consume position regardless of commits) and committing any
			// later record would lose it permanently.
			if err := c.esc.producer.Produce(ctx, dltRec); err != nil {
				c.onError(fmt.Errorf("retry consumer: escalate malformed to DLT: %w", err))
				remaining := make([]*kgo.Record, len(p.Records)-i)
				copy(remaining, p.Records[i:])
				c.holdRecords(tp, remaining, time.Now().Add(escalateRetryDelay))
				return toCommit
			}
			toCommit = append(toCommit, rec)
			continue
		}

		if now.Before(due) {
			// This record is not yet due. Hold it and all remaining records in
			// this partition, pause the partition, and schedule a wake-up timer.
			remaining := make([]*kgo.Record, len(p.Records)-i)
			copy(remaining, p.Records[i:])
			c.holdRecords(tp, remaining, due)
			return toCommit
		}

		// Record is due: process now. On a failed escalation, hold this
		// record and the rest of the batch for a delayed retry — see
		// escalateRetryDelay for why skipping the commit is not enough.
		var failed bool
		toCommit, failed = c.handleRecord(ctx, rec, toCommit)
		if failed {
			remaining := make([]*kgo.Record, len(p.Records)-i)
			copy(remaining, p.Records[i:])
			c.holdRecords(tp, remaining, time.Now().Add(escalateRetryDelay))
			return toCommit
		}
		_ = orig
	}

	return toCommit
}

// holdRecords stores records (in order) for tp until due, pauses the
// partition so no further records are fetched for it, and schedules a wake
// timer. Any previously-held batch/timer for tp is replaced (Fix 4: the seq
// tag makes the old timer's wake event stale).
func (c *Consumer) holdRecords(tp topicPartition, records []*kgo.Record, due time.Time) {
	seq := c.nextSeq.Add(1)

	c.mu.Lock()
	// Stop any existing timer for this partition before replacing.
	if t, exists := c.timers[tp]; exists {
		t.Stop()
	}
	c.held[tp] = &heldBatch{records: records, dueAt: due, seq: seq}
	ev := wakeEvent{tp: tp, seq: seq}
	done := c.done
	t := time.AfterFunc(time.Until(due), func() {
		// Fix 1: always escape via done so no goroutine blocks after Close.
		select {
		case c.wake <- ev:
		case <-done:
		}
	})
	c.timers[tp] = t
	c.mu.Unlock()

	c.client.PauseFetchPartitions(map[string][]int32{tp.topic: {tp.partition}})
}

// handleRecord calls the handler and escalates on failure.
// The record is appended to toCommit when handled or successfully escalated.
// failed=true means the escalation itself failed: the caller MUST stop
// processing the partition and hold the record (plus the rest of the batch)
// for a delayed retry — it is NOT safe to continue, because committing any
// later record would advance past this one and lose it.
func (c *Consumer) handleRecord(ctx context.Context, rec *kgo.Record, toCommit []*kgo.Record) (_ []*kgo.Record, failed bool) {
	r := recordFromKGO(rec)
	_, orig, _, ok := ParseRetryHeaders(r)
	if !ok {
		orig = rec.Topic
	}

	if err := c.handler(ctx, r); err != nil {
		if _, escErr := c.esc.Escalate(ctx, orig, r, err); escErr != nil {
			c.onError(fmt.Errorf("retry consumer: escalate: %w", escErr))
			return toCommit, true
		}
	}

	return append(toCommit, rec), false
}

// Close closes the underlying kgo.Client cleanly, stops all pending timers,
// and signals all timer callbacks via done so they do not block after exit.
func (c *Consumer) Close(_ context.Context) error {
	c.closeOnce.Do(func() {
		// Signal all pending timer callbacks to abandon their wake send.
		close(c.done)

		// Stop and clear all tracked timers.
		c.mu.Lock()
		for tp, t := range c.timers {
			t.Stop()
			delete(c.timers, tp)
		}
		c.mu.Unlock()

		c.client.Close()
	})
	return nil
}

// recordFromKGO converts a *kgo.Record to a kafka.Record.
// This mirrors the identical function in kafka/run.go; it is reproduced here
// to avoid importing an unexported symbol.
func recordFromKGO(rec *kgo.Record) kafka.Record {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	return kafka.Record{
		Topic:     rec.Topic,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   headers,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}
}

// headersFromKGO converts kgo header slice to map.
func headersFromKGO(hs []kgo.RecordHeader) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}
