package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer wraps a *kgo.Client and provides a synchronous Produce method.
type Producer struct {
	cl *kgo.Client
}

// NewProducer returns a Producer that uses cl for all produce operations.
func NewProducer(cl *kgo.Client) *Producer {
	return &Producer{cl: cl}
}

// Produce synchronously writes rec to its topic and waits for broker
// acknowledgment.  The call blocks until the record is durably written or an
// error occurs.
func (p *Producer) Produce(ctx context.Context, rec Record) error {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers))
	for k, v := range rec.Headers {
		headers = append(headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	kr := &kgo.Record{
		Topic:   rec.Topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
	}

	if err := p.cl.ProduceSync(ctx, kr).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce: %w", err)
	}
	return nil
}

// ProduceBatch asynchronously enqueues all records via the franz-go async
// produce API, then calls Flush to wait for all records to be acknowledged by
// the broker in one batched round-trip.
//
// This is the high-throughput counterpart to Produce: instead of one broker
// RTT per record (ProduceSync), ProduceBatch issues a single Flush that lets
// franz-go coalesce all enqueued records into as few broker requests as
// possible — typically one per partition for a same-topic batch.
//
// Error semantics: per-record errors are collected via the async promise
// callbacks and joined into a single error after Flush returns. A non-nil
// error means at least one record was not durably written; callers should treat
// the entire batch as failed (at-least-once: retry the whole batch).
func (p *Producer) ProduceBatch(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)

	wg.Add(len(records))
	for _, rec := range records {
		headers := make([]kgo.RecordHeader, 0, len(rec.Headers))
		for k, v := range rec.Headers {
			headers = append(headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}

		kr := &kgo.Record{
			Topic:   rec.Topic,
			Key:     rec.Key,
			Value:   rec.Value,
			Headers: headers,
		}

		// Enqueue asynchronously; the promise callback fires after the broker
		// acknowledges (or rejects) the record. franz-go batches all enqueued
		// records and sends them in the next Flush call.
		p.cl.Produce(ctx, kr, func(_ *kgo.Record, err error) {
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("kafka: produce batch record: %w", err))
				mu.Unlock()
			}
			wg.Done()
		})
	}

	// Flush blocks until all enqueued records are sent and their promise
	// callbacks have fired, or until ctx is cancelled.
	if err := p.cl.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: produce batch flush: %w", err)
	}

	// Wait for all promise callbacks to complete before reading errs.
	wg.Wait()

	return errors.Join(errs...)
}

// Ping reports whether the underlying client can reach at least one broker.
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}

// Close flushes any buffered records and then closes the client.
// franz-go's Close() does not accept a context, so Flush is called first
// so that the caller's deadline is respected during the flush phase.
//
// The underlying kgo.Client is ALWAYS closed, even if Flush returns an error
// (e.g. context cancellation). Skipping Close() on a Flush error would leak
// the client and its goroutines.
func (p *Producer) Close(ctx context.Context) error {
	flushErr := p.cl.Flush(ctx)
	p.cl.Close() // always release resources, even if flush failed
	if flushErr != nil {
		return fmt.Errorf("kafka: flush: %w", flushErr)
	}
	return nil
}
