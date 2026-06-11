package kafka_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestSASL_SCRAMHandshake proves the franz-go SASL/SCRAM-SHA-256 handshake
// works end-to-end: a NewClient configured from kafka.Config with SCRAM
// credentials authenticates against a SASL-enabled Redpanda, produces a
// record, and reads it back.
//
// Without correct SASL config the broker rejects the connection — the
// produce/consume round-trip succeeding IS the handshake proof.
func TestSASL_SCRAMHandshake(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (SASL redpanda container)")
	}

	broker, creds := kafkatest.NewRedpandaSASL(t)

	ctx := context.Background()

	cfg := kafka.Config{
		Brokers:       []string{broker},
		ClientID:      "test-sasl",
		SASLMechanism: kafka.SASLScramSHA256,
		SASLUser:      creds.User,
		SASLPassword:  config.Secret(creds.Pass),
		// Redpanda's default Kafka listener in this module is plaintext+SASL
		// (PLAINTEXT mechanism over the wire), so no TLS here — A4 documents
		// pairing SASL with TLS for untrusted networks.
	}

	cl, err := kafka.NewClient(cfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err, "NewClient with SCRAM config must succeed")
	defer cl.Close()

	topic := uniqueName("sasl-roundtrip")
	require.NoError(t, kafka.EnsureTopics(ctx, cl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))

	// Produce one record over the authenticated connection.
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte("k"),
		Value: []byte("authenticated"),
	})
	require.NoError(t, res.FirstErr(), "produce over SASL/SCRAM must succeed")

	// Consume it back to confirm the authenticated session is fully usable.
	consumer, err := kafka.NewClient(kafka.Config{
		Brokers:       []string{broker},
		ClientID:      "test-sasl-consumer",
		SASLMechanism: kafka.SASLScramSHA256,
		SASLUser:      creds.User,
		SASLPassword:  config.Secret(creds.Pass),
	}, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer consumer.Close()

	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	fetches := consumer.PollFetches(pollCtx)
	require.NoError(t, fetches.Err())
	recs := fetches.Records()
	require.Len(t, recs, 1, "the authenticated record must round-trip")
	require.Equal(t, "authenticated", string(recs[0].Value))
}

// TestSASL_WrongPassword proves authentication actually gates access: a client
// with a bad password fails to use the SASL-enabled broker.
func TestSASL_WrongPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (SASL redpanda container)")
	}

	broker, creds := kafkatest.NewRedpandaSASL(t)

	ctx := context.Background()

	cl, err := kafka.NewClient(kafka.Config{
		Brokers:       []string{broker},
		ClientID:      "test-sasl-bad",
		SASLMechanism: kafka.SASLScramSHA256,
		SASLUser:      creds.User,
		SASLPassword:  config.Secret("wrong-password"),
	})
	require.NoError(t, err, "construction succeeds; auth fails on first use")
	defer cl.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = cl.Ping(pingCtx)
	require.Error(t, err, "a wrong SASL password must be rejected by the broker")
}
