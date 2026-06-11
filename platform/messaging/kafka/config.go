// Package kafka wraps franz-go (kgo) for produce and consume operations
// with cooperative-sticky consumer groups, otel instrumentation, and
// graceful close.
package kafka

import "go-boilerplate/platform/config"

// SASL mechanism identifiers accepted by Config.SASLMechanism. The empty
// string means "no SASL" (plaintext, back-compatible default).
const (
	SASLPlain       = "PLAIN"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

// Config holds the configuration for a Kafka client.
type Config struct {
	// Brokers is a comma-separated list of seed broker addresses.
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	// ClientID identifies this client to the brokers.
	ClientID string `env:"KAFKA_CLIENT_ID" envDefault:"app"`
	// GroupID is the consumer group this client will join. Empty means
	// no group (producer-only or manual partition assignment).
	GroupID string `env:"KAFKA_GROUP_ID" envDefault:""`

	// SASLMechanism selects the SASL authentication mechanism. Accepted
	// values: "" (none, default — plaintext, back-compatible), "PLAIN",
	// "SCRAM-SHA-256", "SCRAM-SHA-512". Any other value is a startup error.
	SASLMechanism string `env:"KAFKA_SASL_MECHANISM" envDefault:""`
	// SASLUser is the SASL username (ignored when SASLMechanism is empty).
	SASLUser string `env:"KAFKA_SASL_USER" envDefault:""`
	// SASLPassword is the SASL password (ignored when SASLMechanism is empty).
	// config.Secret keeps it out of log/JSON/YAML dumps.
	SASLPassword config.Secret `env:"KAFKA_SASL_PASSWORD" envDefault:""`

	// TLSEnabled wraps the broker connection in TLS. Enable it for any
	// SASL deployment over an untrusted network — SASL/PLAIN and even
	// SCRAM leak material if the transport is plaintext.
	TLSEnabled bool `env:"KAFKA_TLS_ENABLED" envDefault:"false"`
	// TLSInsecureSkipVerify disables broker certificate verification.
	//
	// DEV ONLY. This turns off the only defence against an active
	// man-in-the-middle on the broker connection: with it on, anyone who can
	// intercept the TCP stream can present a forged certificate, terminate
	// TLS, and read/alter every record (and capture SASL credentials). NEVER
	// set this true outside a local/test environment — provision a proper CA
	// bundle instead.
	TLSInsecureSkipVerify bool `env:"KAFKA_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`
}
