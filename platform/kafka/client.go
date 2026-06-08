package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
)

// NewClient returns a new *kgo.Client configured from cfg.
// Extra opts are appended after the standard options, allowing callers to
// layer in consumer-group settings, custom partitioners, etc.
//
// Idempotent producer behaviour is the franz-go default (RequiredAcks=-1,
// idempotency enabled).  OpenTelemetry tracing is wired via kotel using the
// global TracerProvider and Propagator.
func NewClient(cfg Config, extra ...kgo.Opt) (*kgo.Client, error) {
	kt := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()),
	)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithHooks(kt.Hooks()...),
	}
	opts = append(opts, extra...)

	return kgo.NewClient(opts...)
}
