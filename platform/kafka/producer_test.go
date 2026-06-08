package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/platform/kafka"
	"go-boilerplate/platform/kafka/kafkatest"
)

func TestProducer_ProduceAndConsumeRoundTrip(t *testing.T) {
	broker, _ := kafkatest.NewRedpanda(t)

	ctx := context.Background()

	// Build producer client.
	producerCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "test-producer",
	})
	require.NoError(t, err)
	defer producerCl.Close()

	const topic = "test-topic"

	// Ensure the topic exists before producing.
	require.NoError(t, kafka.EnsureTopics(ctx, producerCl, 1, 1, topic))

	prod := kafka.NewProducer(producerCl)
	require.NoError(t, prod.Ping(ctx))

	err = prod.Produce(ctx, kafka.Record{
		Topic:   topic,
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: map[string]string{"h": "1"},
	})
	require.NoError(t, err)

	// Flush + close producer before reading so the record is committed.
	require.NoError(t, prod.Close(ctx))

	// Build consumer client.
	consumerCl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ClientID("test-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumerCl.Close()

	// Poll with a deadline so the test does not hang on failure.
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) == 0 {
		fetches := consumerCl.PollFetches(pollCtx)
		require.NoError(t, fetches.Err())
		records = append(records, fetches.Records()...)
	}

	require.Len(t, records, 1)
	rec := records[0]
	assert.Equal(t, []byte("k"), rec.Key)
	assert.Equal(t, []byte("v"), rec.Value)

	// Verify the header.
	var headerVal string
	for _, h := range rec.Headers {
		if h.Key == "h" {
			headerVal = string(h.Value)
			break
		}
	}
	assert.Equal(t, "1", headerVal, "expected header h=1")
}
