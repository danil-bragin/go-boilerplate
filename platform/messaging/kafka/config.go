// Package kafka wraps franz-go (kgo) for produce and consume operations
// with cooperative-sticky consumer groups, otel instrumentation, and
// graceful close.
package kafka

// Config holds the configuration for a Kafka client.
type Config struct {
	// Brokers is a comma-separated list of seed broker addresses.
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	// ClientID identifies this client to the brokers.
	ClientID string `env:"KAFKA_CLIENT_ID" envDefault:"app"`
	// GroupID is the consumer group this client will join. Empty means
	// no group (producer-only or manual partition assignment).
	GroupID string `env:"KAFKA_GROUP_ID" envDefault:""`
}
