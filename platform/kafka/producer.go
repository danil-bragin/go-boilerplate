package kafka

import (
	"context"
	"fmt"

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
func (p *Producer) Close(ctx context.Context) error {
	if err := p.cl.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}
	p.cl.Close()
	return nil
}
