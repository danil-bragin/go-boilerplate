package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Backoff bounds for a partition whose handler keeps failing. The first
// failure pauses the partition for redeliveryBackoffFloor; each consecutive
// failure doubles the pause up to redeliveryBackoffCap. The backoff resets
// once a poll completes for the partition with progress and NO new failure;
// a poll in which some records succeed but a later record fails keeps the
// accumulated failure count, so the pause keeps escalating until a fully
// clean poll.
const (
	redeliveryBackoffFloor = 100 * time.Millisecond
	redeliveryBackoffCap   = 5 * time.Second
)

// tp identifies a topic-partition.
type tp struct {
	topic     string
	partition int32
}

// backoffState tracks consecutive handler failures for one partition.
// until is the zero time when the partition is currently fetchable (resumed
// and awaiting the retry outcome) and non-zero while the partition is paused.
type backoffState struct {
	failures int
	until    time.Time
}

// Run enters a poll loop, processing partitions concurrently within each poll
// and committing all successful offsets in a single batch RPC per poll.
//
// Concurrency model:
//   - Each call to PollFetches may return records from multiple partitions.
//   - One goroutine is launched per partition (via sync.WaitGroup). Within a
//     partition records are processed SEQUENTIALLY, preserving per-partition
//     ordering.
//   - After all partition goroutines finish, the last successfully-handled
//     record from EACH partition is committed in a single CommitRecords call
//     (one OffsetCommit RPC for the whole poll instead of one per record).
//   - kgo.BlockRebalanceOnPoll() ensures no rebalance can occur between
//     PollFetches and AllowRebalance(), so concurrent processing and the
//     seek-back below are safe.
//     AllowRebalance() is called after every commit, even on error.
//
// Failure semantics (at-least-once, per-partition):
//   - Within a partition, on the first handler error processing stops for that
//     partition and the consumer SEEKS BACK to the failed record
//     (kgo.Client.SetOffsets) so the next fetch redelivers it. kgo advances
//     its in-memory consume position on PollFetches regardless of commits, so
//     without the explicit seek a failed record would be skipped permanently
//     and a later commit would advance past it — silent message loss.
//   - The failed partition is paused with exponential backoff
//     (redeliveryBackoffFloor .. redeliveryBackoffCap) so a poison record cannot
//     hot-loop the consumer. Other partitions are unaffected and commit their
//     own progress independently. Permanent poison handling (bounded retries +
//     DLT) is layered on top via WithRetry (dlq.go).
//
// Duplicate-delivery window:
//   - There is a small window between a successful handler return and the
//     CommitRecords RPC completing. If the process crashes or a rebalance
//     occurs in that window the record will be redelivered after the partition
//     is reassigned. Consumers MUST be idempotent and deduplicate by a stable
//     idempotency key (e.g. an inbox table keyed on message-id).
//
// The loop stops when ctx is cancelled; Run returns ctx.Err() in that case.
// It also returns if the underlying client is closed (ErrClientClosed).
func (c *Consumer) Run(ctx context.Context, h HandlerFunc) error {
	// Per-partition redelivery backoff state. Entries are created on handler
	// failure and removed on the next successful record from that partition.
	// A partition revoked while present here is harmless: the map is bounded
	// by the number of partitions ever assigned, and redelivery after
	// reassignment starts from the committed offset anyway.
	backoffs := map[tp]*backoffState{}

	for {
		// Resume partitions whose backoff has expired, and find the earliest
		// pending resume to bound the poll below.
		now := time.Now()
		var (
			resume   map[string][]int32
			earliest time.Time
		)
		for key, st := range backoffs {
			if st.until.IsZero() {
				continue // already fetchable, awaiting retry outcome
			}
			if !st.until.After(now) {
				if resume == nil {
					resume = map[string][]int32{}
				}
				resume[key.topic] = append(resume[key.topic], key.partition)
				st.until = time.Time{}
				continue
			}
			if earliest.IsZero() || st.until.Before(earliest) {
				earliest = st.until
			}
		}
		if resume != nil {
			c.cl.ResumeFetchPartitions(resume)
		}

		// While any partition is paused for backoff, bound the poll so the
		// loop wakes up in time to resume it even if no other records arrive.
		pollCtx := ctx
		var cancelPoll context.CancelFunc
		if !earliest.IsZero() {
			pollCtx, cancelPoll = context.WithDeadline(ctx, earliest)
		}

		fetches := c.cl.PollFetches(pollCtx)
		if cancelPoll != nil {
			cancelPoll()
		}

		// Stop if the caller cancelled the context.
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			return ctx.Err()
		}

		// Stop if the client itself has been closed.
		if fetches.IsClientClosed() {
			c.cl.AllowRebalance()
			return nil
		}

		// Handle fetch-level errors. Caller cancellation and client-closed are
		// handled above; a deadline from the bounded pollCtx is the expected
		// backoff wake-up, not an error. Other errors are non-fatal for the
		// poll loop.
		for _, fe := range fetches.Errors() {
			switch {
			case errors.Is(fe.Err, context.Canceled), errors.Is(fe.Err, context.DeadlineExceeded):
				// pollCtx deadline (backoff wake-up): benign, keep looping.
			case errors.Is(fe.Err, kgo.ErrClientClosed):
				c.cl.AllowRebalance()
				return nil
			default:
				// Non-fatal for the poll loop (errored partitions are skipped
				// by the iterator; kgo retries internally), but surfaced so
				// persistent fetch problems are visible.
				c.onError(ctx, StageFetch, fe.Err)
			}
		}

		// Collect, per partition: the last successfully-handled record and the
		// first failed record (for seek-back). mu guards both; each goroutine
		// writes after finishing, so contention is minimal.
		var (
			mu       sync.Mutex
			lastGood []*kgo.Record
			failed   map[tp]*kgo.Record
		)

		// Launch one goroutine per partition; preserve per-partition ordering.
		var wg sync.WaitGroup

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// Record consumer lag for this partition from the fetch's high
			// watermark: lag = HWM − (last fetched offset + 1). This runs on
			// the poll goroutine before the workers start.
			if n := len(p.Records); n > 0 {
				lastOff := p.Records[n-1].Offset
				c.metrics.recordLag(ctx, p.Topic, p.Partition, p.HighWatermark-lastOff-1)
			}

			// Capture by value so the closure is safe to run concurrently.
			part := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				var (
					last      *kgo.Record
					firstBad  *kgo.Record
					succeeded int64
				)
				part.EachRecord(func(rec *kgo.Record) {
					if firstBad != nil {
						// A previous record in this partition failed; skip the
						// rest — the seek-back below rewinds to firstBad.
						return
					}
					r := RecordFromKGO(rec)
					start := time.Now()
					err := h(ctx, r)
					c.metrics.recordHandlerDuration(ctx, part.Topic, time.Since(start), err)
					if err != nil {
						firstBad = rec
						return
					}
					succeeded++
					last = rec
				})
				c.metrics.addProcessed(ctx, part.Topic, succeeded)
				if firstBad != nil {
					c.metrics.addFailed(ctx, part.Topic)
				}
				mu.Lock()
				defer mu.Unlock()
				if last != nil {
					lastGood = append(lastGood, last)
				}
				if firstBad != nil {
					if failed == nil {
						failed = map[tp]*kgo.Record{}
					}
					failed[tp{firstBad.Topic, firstBad.Partition}] = firstBad
				}
			}()
		})

		// Wait for all partition goroutines to finish.
		wg.Wait()

		// Commit all successfully-processed partitions in one RPC.
		if len(lastGood) > 0 {
			c.commit(ctx, lastGood)
		}

		// Clear backoff state for partitions that made progress without a new
		// failure: the retried record succeeded.
		for _, rec := range lastGood {
			key := tp{rec.Topic, rec.Partition}
			if _, alsoFailed := failed[key]; !alsoFailed {
				delete(backoffs, key)
			}
		}

		// Seek failed partitions back to the failing record and pause them
		// with exponential backoff. Both calls are safe here: the rebalance is
		// still blocked (BlockRebalanceOnPoll) until AllowRebalance below.
		if len(failed) > 0 {
			pause := map[string][]int32{}
			seek := map[string]map[int32]kgo.EpochOffset{}
			now = time.Now()
			for key, rec := range failed {
				st := backoffs[key]
				if st == nil {
					st = &backoffState{}
					backoffs[key] = st
				}
				st.failures++
				delay := redeliveryBackoffFloor << (st.failures - 1)
				if delay <= 0 || delay > redeliveryBackoffCap {
					delay = redeliveryBackoffCap
				}
				st.until = now.Add(delay)

				pause[key.topic] = append(pause[key.topic], key.partition)
				if seek[key.topic] == nil {
					seek[key.topic] = map[int32]kgo.EpochOffset{}
				}
				seek[key.topic][key.partition] = kgo.EpochOffset{
					Epoch:  rec.LeaderEpoch,
					Offset: rec.Offset,
				}
			}
			c.cl.PauseFetchPartitions(pause)
			c.cl.SetOffsets(seek)
		}

		// Allow the next rebalance now that we have committed and sought.
		c.cl.AllowRebalance()
	}
}

// commit commits the given records, surviving caller cancellation: during
// shutdown (ctx already cancelled) the commit runs on a fresh detached
// 5-second context so the final batch of successfully-processed offsets is
// not dropped — dropping it would redeliver the whole last poll on every
// deploy. Commit errors are surfaced via the WithOnError callback.
func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) {
	commitCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		commitCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
	}
	if err := c.cl.CommitRecords(commitCtx, recs...); err != nil {
		seen := map[string]struct{}{}
		for _, rec := range recs {
			if _, ok := seen[rec.Topic]; ok {
				continue
			}
			seen[rec.Topic] = struct{}{}
			c.metrics.addCommitFailure(ctx, rec.Topic)
		}
		c.onError(ctx, StageCommit, err)
	}
}
