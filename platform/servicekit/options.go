package servicekit

// Option customizes New. The default Service wires Postgres AND Kafka; the
// Without* options switch those subsystems off entirely for services that do
// not need them (e.g. a pure HTTP edge or a stateless worker), so New never
// dials the corresponding endpoint and the related readiness checks are not
// registered.
type Option func(*options)

type options struct {
	withoutKafka bool
	withoutPG    bool
}

// WithoutKafka builds the Service with no Kafka client and no producer:
// New does not construct (or connect) a kgo client, registers no kafka
// readiness check, and the kafka-dependent adders (EnsureTopics, AddConsumer,
// AddConsumerWithRetry) return an error if called.
func WithoutKafka() Option {
	return func(o *options) { o.withoutKafka = true }
}

// WithoutPG builds the Service with no Postgres pool: New does not dial
// Postgres, runs no migrations, registers no postgres readiness check, and
// skips the inbox-cleanup loop. The pg-dependent adders (AddOutboxRelay)
// return an error if called, and Pool() returns nil.
func WithoutPG() Option {
	return func(o *options) { o.withoutPG = true }
}
