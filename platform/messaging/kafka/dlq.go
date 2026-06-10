package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// RetryOpts configures WithRetry.
type RetryOpts struct {
	// MaxAttempts is the total number of handler invocations before the record
	// is routed to the dead-letter topic. Values <= 0 are treated as 1.
	MaxAttempts int

	// Producer is used to write poison messages to the dead-letter topic.
	Producer *Producer

	// DLTTopic returns the dead-letter topic name for a given source topic.
	// Defaults to func(t string) string { return t + ".DLT" } when nil.
	DLTTopic func(topic string) string

	// Backoff is the base sleep duration between in-process retry attempts.
	// Values <= 0 are treated as 50 ms.
	Backoff time.Duration
}

// WithRetry wraps a HandlerFunc: it retries the handler up to
// opts.MaxAttempts times in-process (with ctx-aware backoff).  If all
// attempts fail the record is produced to the dead-letter topic (with
// x-error, x-attempts, and x-original-topic headers preserved) and the
// wrapper returns nil so the consumer commits the offset — the poison
// message is parked in the DLT and no longer blocks the partition.
//
// NOTE: in-process retry blocks the partition during the retry window —
// but it never reorders per-key records. For non-blocking tiered
// retry-topics (which DO trade away per-key ordering) see
// platform/messaging/retry.
//
// Context cancellation:
//   - If ctx is cancelled while sleeping between attempts the wrapper
//     returns ctx.Err() immediately (no DLT write).
//   - If ctx is cancelled before the very first attempt the first call to
//     h is still made; cancellation is only checked in the sleep between
//     attempts.
func WithRetry(h HandlerFunc, opts RetryOpts) HandlerFunc {
	// Fail fast at wiring time: a nil Producer would panic at runtime only
	// when the first poison message reaches the DLT path, which is hard to
	// reproduce in tests and catastrophic in production.
	if opts.Producer == nil {
		panic("kafka: WithRetry requires a non-nil Producer for dead-letter routing")
	}

	// Apply defaults.
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 1
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 50 * time.Millisecond
	}
	if opts.DLTTopic == nil {
		opts.DLTTopic = func(t string) string { return t + ".DLT" }
	}

	metrics := newDLTMetrics()

	return func(ctx context.Context, rec Record) error {
		var lastErr error

		for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
			lastErr = h(ctx, rec)
			if lastErr == nil {
				return nil
			}

			// Handler failed. If more attempts remain, sleep with ctx awareness.
			if attempt < opts.MaxAttempts {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(opts.Backoff):
					// continue to next attempt
				}
			}
		}

		// All attempts exhausted — check whether ctx was cancelled before
		// attempting the DLT write.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Build the DLT record. Copy headers from the original record and
		// add diagnostic metadata.
		dltHeaders := make(map[string]string, len(rec.Headers)+3)
		for k, v := range rec.Headers {
			dltHeaders[k] = v
		}
		dltHeaders[HeaderDLTError] = lastErr.Error()
		dltHeaders[HeaderDLTAttempts] = strconv.Itoa(opts.MaxAttempts)
		dltHeaders[HeaderDLTOriginalTopic] = rec.Topic

		dltRec := Record{
			Topic:   opts.DLTTopic(rec.Topic),
			Key:     rec.Key,
			Value:   rec.Value,
			Headers: dltHeaders,
		}

		if err := opts.Producer.Produce(ctx, dltRec); err != nil {
			// DLT produce failed — do NOT commit; return the error so the
			// consumer retries later.  This is preferable to silently losing
			// the message.
			return fmt.Errorf("kafka: dlq: produce to DLT %s: %w", dltRec.Topic, err)
		}

		// Successful DLT write — return nil so the consumer commits the offset.
		metrics.addProduced(ctx, dltRec.Topic)
		return nil
	}
}
