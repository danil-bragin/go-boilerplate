// Command redrive republishes dead-letter-topic (DLT) records back to their
// original topics so they can be reprocessed after the underlying failure is
// fixed.
//
// Each DLT record must carry its original topic in either the
// "x-original-topic" header (written by kafka.WithRetry) or the
// "retry-orig-topic" header (written by the tiered retry escalator); a record
// with neither aborts the run — by design, nothing is guessed and nothing is
// committed past an unreadable record.
//
// Retry/diagnostic headers (x-error, x-attempts, x-original-topic,
// retry-attempt, retry-orig-topic, retry-due-at, retry-last-error) are
// stripped before republishing so the record re-enters the pipeline as a
// clean first attempt.
//
// # Replay modes
//
// By default the original "message-id" header is preserved: consumers that
// already processed the message before it dead-lettered (e.g. a multi-handler
// group where only one handler failed) deduplicate via the inbox and skip it.
// With --fresh-ids a NEW message-id is minted per record, bypassing inbox
// dedup — use this for projection rebuilds where the side effect must run
// again on purpose.
//
// # Progress and reruns
//
// The reader joins consumer group "redrive" (override with --group) and
// commits each DLT offset only AFTER the record was successfully republished.
// An interrupted run therefore resumes where it left off; --dry-run commits
// nothing and only lists what would happen.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config controls one redrive run. See the package documentation for the
// semantics of each mode.
type Config struct {
	Brokers  []string  // Kafka bootstrap brokers (required)
	DLT      string    // dead-letter topic to drain (required)
	Limit    int       // max records to process; 0 = all pending
	DryRun   bool      // list only: no republish, no commit
	FreshIDs bool      // mint new message-id headers (bypass inbox dedup)
	Group    string    // consumer group for progress; default "redrive"
	Out      io.Writer // listing/summary output; default io.Discard when nil
}

// Stats summarises a redrive run.
type Stats struct {
	Read        int // DLT records examined
	Republished int // records produced to their original topic (and committed)
}

// strippedHeaders are removed from every record before republishing: the
// record re-enters the pipeline as a clean first attempt.
var strippedHeaders = map[string]bool{
	"x-error":          true,
	"x-attempts":       true,
	"x-original-topic": true,
	"retry-attempt":    true,
	"retry-orig-topic": true,
	"retry-due-at":     true,
	"retry-last-error": true,
}

// pollTimeout bounds one PollFetches call; pending records not delivered
// within it abort the run (broker trouble — rerun resumes from the commit).
const pollTimeout = 30 * time.Second

// Run drains the DLT according to cfg. It returns the run stats and the
// first error encountered; on error nothing past the last successfully
// republished record has been committed, so a rerun continues safely.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	var stats Stats
	if len(cfg.Brokers) == 0 || cfg.DLT == "" {
		return stats, errors.New("redrive: --brokers and --dlt are required")
	}
	group := cfg.Group
	if group == "" {
		group = "redrive"
	}
	out := cfg.Out
	if out == nil {
		out = io.Discard
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.DLT),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return stats, fmt.Errorf("redrive: building kafka client: %w", err)
	}
	defer cl.Close()

	pending, err := pendingCount(ctx, cl, cfg.DLT, group)
	if err != nil {
		return stats, err
	}
	if pending == 0 {
		_, _ = fmt.Fprintf(out, "redrive: %s has no pending records for group %q\n", cfg.DLT, group)
		return stats, nil
	}
	target := pending
	if cfg.Limit > 0 && int64(cfg.Limit) < target {
		target = int64(cfg.Limit)
	}

	for int64(stats.Read) < target {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if err := fetches.Err0(); err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			if ctx.Err() != nil {
				return stats, ctx.Err()
			}
			return stats, fmt.Errorf("redrive: timed out waiting for %d pending records (read %d)", target, stats.Read)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return stats, fmt.Errorf("redrive: fetch: %w", errs[0].Err)
		}

		for _, rec := range fetches.Records() {
			if int64(stats.Read) >= target {
				break
			}
			stats.Read++
			if err := redriveRecord(ctx, cl, cfg, out, rec); err != nil {
				return stats, err
			}
			if !cfg.DryRun {
				stats.Republished++
			}
		}
	}

	_, _ = fmt.Fprintf(out, "redrive: done — read %d, republished %d (dry-run=%v, fresh-ids=%v)\n",
		stats.Read, stats.Republished, cfg.DryRun, cfg.FreshIDs)
	return stats, nil
}

// redriveRecord republishes one DLT record to its original topic and commits
// its offset; in dry-run mode it only prints what would happen.
func redriveRecord(ctx context.Context, cl *kgo.Client, cfg Config, out io.Writer, rec *kgo.Record) error {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}

	origTopic := headers["x-original-topic"]
	if origTopic == "" {
		origTopic = headers["retry-orig-topic"]
	}
	if origTopic == "" {
		return fmt.Errorf(
			"redrive: record %s[%d]@%d has neither x-original-topic nor retry-orig-topic header — cannot determine destination (aborting; nothing committed past the previous record)",
			rec.Topic, rec.Partition, rec.Offset)
	}

	outHeaders := make([]kgo.RecordHeader, 0, len(rec.Headers))
	for _, h := range rec.Headers {
		if strippedHeaders[h.Key] {
			continue
		}
		if cfg.FreshIDs && h.Key == "message-id" {
			continue
		}
		outHeaders = append(outHeaders, h)
	}
	if cfg.FreshIDs {
		outHeaders = append(outHeaders, kgo.RecordHeader{Key: "message-id", Value: []byte(uuid.New().String())})
	}

	if cfg.DryRun {
		_, _ = fmt.Fprintf(out, "DRY-RUN %s[%d]@%d -> %s key=%q message-id=%q event-type=%q\n",
			rec.Topic, rec.Partition, rec.Offset, origTopic,
			string(rec.Key), headers["message-id"], headers["event-type"])
		return nil
	}

	newRec := &kgo.Record{
		Topic:   origTopic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: outHeaders,
	}
	if err := cl.ProduceSync(ctx, newRec).FirstErr(); err != nil {
		return fmt.Errorf("redrive: republish %s[%d]@%d to %s: %w", rec.Topic, rec.Partition, rec.Offset, origTopic, err)
	}
	// Commit only AFTER the republish succeeded: an interrupted run re-reads
	// (and re-publishes — at-least-once) anything past the last commit.
	if err := cl.CommitRecords(ctx, rec); err != nil {
		return fmt.Errorf("redrive: commit %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	_, _ = fmt.Fprintf(out, "redrive %s[%d]@%d -> %s key=%q\n",
		rec.Topic, rec.Partition, rec.Offset, origTopic, string(rec.Key))
	return nil
}

// pendingCount computes how many DLT records the group has not yet processed:
// sum over partitions of end − max(committed, start). The snapshot is taken
// once at startup — records produced to the DLT during the run are left for
// the next run.
func pendingCount(ctx context.Context, cl *kgo.Client, topic, group string) (int64, error) {
	adm := kadm.NewClient(cl)

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("redrive: list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("redrive: list end offsets: %w", err)
	}
	committed, err := adm.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		return 0, fmt.Errorf("redrive: fetch committed offsets: %w", err)
	}

	var pending int64
	ends.Each(func(end kadm.ListedOffset) {
		from := int64(0)
		if s, ok := starts.Lookup(end.Topic, end.Partition); ok {
			from = s.Offset
		}
		if c, ok := committed.Lookup(end.Topic, end.Partition); ok && c.At > from {
			from = c.At
		}
		if end.Offset > from {
			pending += end.Offset - from
		}
	})
	return pending, nil
}
