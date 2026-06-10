package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
)

// TransactConsumer provides exactly-once consume-process-produce for PURE
// kafka→kafka pipelines via Kafka transactions: input offsets and output
// records commit atomically, and aborted batches' outputs are invisible to
// read-committed consumers.
//
// BOUNDARY: a Kafka transaction cannot span a database. Any pipeline that
// writes to Postgres must use the transactional outbox instead — see
// docs/adr/0006-kafka-eos-boundaries.md.
//
// Balancer note: cooperative-sticky is fully supported by GroupTransactSession
// in franz-go v1.21+. The session's OnPartitionsRevoked hook distinguishes a
// cooperative revoke of zero partitions (no abort needed) from an actual
// revoke, so cooperative rebalancing does not force unnecessary aborts.
// Range balancer also works; cooperative-sticky is the default because it
// reduces partition churn during rolling deployments.
type TransactConsumer struct {
	sess    *kgo.GroupTransactSession
	log     *slog.Logger
	onError func(error)
}

// TransactOption configures a TransactConsumer.
type TransactOption func(*TransactConsumer)

// WithTransactLogger sets the structured logger. Defaults to slog.Default().
func WithTransactLogger(l *slog.Logger) TransactOption {
	return func(t *TransactConsumer) { t.log = l }
}

// WithTransactOnError sets a callback invoked for non-fatal operational errors
// (e.g. a batch that was aborted due to a rebalance). Defaults to a no-op.
func WithTransactOnError(fn func(error)) TransactOption {
	return func(t *TransactConsumer) { t.onError = fn }
}

// NewTransactConsumer creates a TransactConsumer that joins groupID, consumes
// topics, and produces transactionally under txnID.
//
// The underlying GroupTransactSession is built with:
//   - seed brokers and client-id from cfg (same as NewClient)
//   - OTel tracing via kotel
//   - kgo.TransactionalID(txnID) — identifies the producer epoch for EOS
//   - kgo.ConsumerGroup(groupID)
//   - kgo.ConsumeTopics(topics...)
//   - kgo.Balancers(kgo.CooperativeStickyBalancer())
//   - kgo.FetchIsolationLevel(kgo.ReadCommitted()) — skips aborted records
//   - RequireStableFetchOffsets is permanently on in franz-go v1.21+ (no call needed)
//   - kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()) — new group reads from
//     beginning so no events are missed on first startup
func NewTransactConsumer(
	cfg Config,
	txnID, groupID string,
	topics []string,
	opts ...TransactOption,
) (*TransactConsumer, error) {
	if txnID == "" {
		return nil, errors.New("kafka: NewTransactConsumer: txnID must not be empty")
	}
	if groupID == "" {
		return nil, errors.New("kafka: NewTransactConsumer: groupID must not be empty")
	}
	if len(topics) == 0 {
		return nil, errors.New("kafka: NewTransactConsumer: at least one topic must be provided")
	}

	kt := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()),
	)

	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithHooks(kt.Hooks()...),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// RequireStableFetchOffsets is permanently enabled in franz-go v1.21+
		// (no-op call omitted; behaviour is always on per upstream docs).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: NewTransactConsumer: %w", err)
	}

	tc := &TransactConsumer{
		sess:    sess,
		log:     slog.Default(),
		onError: func(error) {},
	}
	for _, o := range opts {
		o(tc)
	}
	return tc, nil
}

// ProcessFn maps one input record to zero or more output records produced
// within the same transaction. If ProcessFn returns a non-nil error the batch
// is aborted and the input records will be redelivered.
//
// SERIAL PROCESSING: Run calls ProcessFn for the records of a batch one at a
// time, in fetch order, on a single goroutine. There is no per-partition
// parallelism in the transactional path — a slow ProcessFn directly bounds
// pipeline throughput. This is a deliberate trade-off: a Kafka transaction is
// a single unit, so partial-batch parallelism would not improve commit
// latency, and serial execution keeps the abort semantics trivial. Use the
// regular Consumer (at-least-once + idempotent handler) when per-partition
// concurrency matters more than exactly-once output atomicity.
type ProcessFn func(ctx context.Context, rec Record) ([]Record, error)

// Run enters the transactional poll loop. For each fetched batch it:
//  1. Calls sess.Begin() to open a Kafka transaction.
//  2. Calls fn for every record; on error aborts the transaction so all
//     produced outputs of this batch are invisible to read-committed consumers.
//  3. Waits for every produced output record's promise; any produce error —
//     including CLIENT-SIDE failures that never reach the broker (record too
//     large, unknown topic past the retry limit, delivery timeout) — also
//     forces an abort. kgo does not abort these on its own: only fatal
//     producer-ID errors poison a transaction.
//  4. Calls sess.End(ctx, TryCommit/TryAbort) to atomically commit both the
//     output records and the input offsets.
//
// If a batch is aborted (fn error, produce error, or rebalance) the input
// offsets are NOT committed, so the broker redelivers the batch — preserving
// exactly-once semantics over the whole pipeline.
//
// Run returns when ctx is cancelled or the underlying client is closed.
func (t *TransactConsumer) Run(ctx context.Context, fn ProcessFn) error {
	for {
		// PollFetches blocks until records arrive or ctx is done.
		fetches := t.sess.PollFetches(ctx)

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetches.IsClientClosed() {
			return nil
		}

		// Handle fetch-level errors (non-fatal for the poll loop).
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				return fe.Err
			}
			if errors.Is(fe.Err, kgo.ErrClientClosed) {
				return nil
			}
			// Swallow transient errors; errored partitions are skipped by the iterator.
			t.onError(fmt.Errorf("kafka: TransactConsumer: fetch error: %w", fe.Err))
		}

		// No records in this poll — nothing to transact.
		if fetches.NumRecords() == 0 {
			continue
		}

		// ── Begin transaction ────────────────────────────────────────────────
		if err := t.sess.Begin(); err != nil {
			// Begin fails only if no transactional ID or already in a transaction.
			// Both are programming errors; surface them as fatal.
			return fmt.Errorf("kafka: TransactConsumer: Begin: %w", err)
		}

		// ── Process all records in this batch ────────────────────────────────
		ok := true

		// Per-record produce-error gate (mirrors franz-go's own EOS example).
		// kgo does NOT auto-abort a transaction whose records failed
		// CLIENT-SIDE (e.g. kerr.MessageTooLarge at buffering time, unknown
		// topic past the retry limit, delivery timeout): only fatal
		// producer-ID errors poison the transaction. End(TryCommit) would
		// flush nothing for already-failed records and happily commit the
		// input offsets — silently losing the output. The promise records the
		// first produce error and immediately aborts all other buffered
		// records; the end decision below is gated on it.
		fep := kgo.AbortingFirstErrPromise(t.sess.Client())

		fetches.EachRecord(func(rec *kgo.Record) {
			if !ok {
				return // batch already poisoned; skip remaining records
			}

			outs, err := fn(ctx, RecordFromKGO(rec))
			if err != nil {
				ok = false
				t.onError(fmt.Errorf("kafka: TransactConsumer: ProcessFn: %w", err))
				return
			}

			for _, out := range outs {
				t.sess.Produce(ctx, recordToKGO(out), fep.Promise())
			}
		})

		// Err waits for every outstanding produce promise to complete, so the
		// commit decision sees the final state of all output records. On the
		// first error the promise has already aborted the remaining buffered
		// records, so this wait is short in the failure case.
		produceErr := fep.Err()
		if produceErr != nil {
			ok = false
			t.onError(fmt.Errorf("kafka: TransactConsumer: produce: %w", produceErr))
		}

		// ── End transaction ──────────────────────────────────────────────────
		// Commit only when BOTH ProcessFn and every produce succeeded.
		committed, err := t.sess.End(ctx, kgo.TransactionEndTry(ok))
		if err != nil {
			// End errors are non-retryable per franz-go docs; surface and stop.
			return fmt.Errorf("kafka: TransactConsumer: End: %w", err)
		}

		if !committed {
			// Aborted (fn error, produce error, or a rebalance). The broker
			// will redeliver the batch.
			t.log.InfoContext(
				ctx, "kafka: TransactConsumer: transaction aborted; batch will be redelivered",
				"try_commit", ok,
			)
		}
	}
}

// Close closes the underlying GroupTransactSession cleanly, leaving the
// consumer group. The ctx parameter is accepted for interface consistency with
// run.TeardownFunc but is not forwarded because kgo.GroupTransactSession.Close
// is synchronous.
func (t *TransactConsumer) Close(_ context.Context) error {
	t.sess.Close()
	return nil
}

// recordToKGO converts a broker-agnostic Record to a *kgo.Record.
func recordToKGO(rec Record) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers))
	for k, v := range rec.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return &kgo.Record{
		Topic:   rec.Topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
	}
}
