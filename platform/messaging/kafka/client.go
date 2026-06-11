package kafka

import (
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kotel"
)

// NewClient returns a new *kgo.Client configured from cfg.
// Extra opts are appended after the standard options, allowing callers to
// layer in consumer-group settings, custom partitioners, etc.
//
// Idempotent producer behaviour is the franz-go default (RequiredAcks=-1,
// idempotency enabled).  OpenTelemetry tracing is wired via kotel using the
// global TracerProvider and Propagator.
//
// SASL and TLS are wired from cfg (see saslTLSOpts): an empty SASLMechanism
// leaves the connection unauthenticated (back-compatible plaintext default);
// TLSEnabled=false leaves it cleartext.
func NewClient(cfg Config, extra ...kgo.Opt) (*kgo.Client, error) {
	kt := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()),
	)

	secOpts, err := saslTLSOpts(cfg)
	if err != nil {
		return nil, err
	}

	opts := make([]kgo.Opt, 0, 3+len(secOpts)+len(extra))
	opts = append(
		opts,
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithHooks(kt.Hooks()...),
	)
	opts = append(opts, secOpts...)
	opts = append(opts, extra...)

	return kgo.NewClient(opts...)
}

// saslTLSOpts maps the security-related fields of cfg to franz-go options.
//
// Mapping:
//
//	SASLMechanism ""              → no kgo.SASL option (plaintext)
//	SASLMechanism "PLAIN"         → kgo.SASL(plain.Auth{...}.AsMechanism())
//	SASLMechanism "SCRAM-SHA-256" → kgo.SASL(scram.Auth{...}.AsSha256Mechanism())
//	SASLMechanism "SCRAM-SHA-512" → kgo.SASL(scram.Auth{...}.AsSha512Mechanism())
//	TLSEnabled true               → kgo.DialTLSConfig(&tls.Config{MinVersion: TLS12, InsecureSkipVerify: cfg.TLSInsecureSkipVerify})
//
// An unrecognised SASLMechanism is a configuration error (fail closed).
func saslTLSOpts(cfg Config) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	switch cfg.SASLMechanism {
	case "":
		// No SASL — plaintext, back-compatible default.
	case SASLPlain:
		opts = append(opts, kgo.SASL(plain.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword.Reveal(),
		}.AsMechanism()))
	case SASLScramSHA256:
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword.Reveal(),
		}.AsSha256Mechanism()))
	case SASLScramSHA512:
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword.Reveal(),
		}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf(
			"kafka: unsupported KAFKA_SASL_MECHANISM %q (want one of %q, %q, %q, or empty)",
			cfg.SASLMechanism, SASLPlain, SASLScramSHA256, SASLScramSHA512,
		)
	}

	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.TLSInsecureSkipVerify, //nolint:gosec // G402: dev-only escape hatch, loud doc on Config.TLSInsecureSkipVerify
		}))
	}

	return opts, nil
}
