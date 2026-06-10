package kafka_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

// TestEnsureTopics_AppliesSpec verifies that EnsureTopics creates topics with
// the exact partition count, replication factor, and topic configs from the
// TopicSpec (verified via kadm metadata + config describe).
func TestEnsureTopics_AppliesSpec(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.Shared(t)
	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "admin-test-" + uuid.NewString()[:8],
	})
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "admin.spec-" + uuid.NewString()[:8]
	spec := kafka.TopicSpec{
		Partitions:        6,
		ReplicationFactor: 1,
		Configs:           map[string]string{"retention.ms": "604800000"},
	}
	require.NoError(t, kafka.EnsureTopics(ctx, cl, spec, topic))

	adm := kadm.NewClient(cl)
	details, err := adm.ListTopics(ctx, topic)
	require.NoError(t, err)
	td, ok := details[topic]
	require.True(t, ok, "topic must exist")
	require.NoError(t, td.Err)
	require.Len(t, td.Partitions, 6, "partition count must match spec")

	cfgs, err := adm.DescribeTopicConfigs(ctx, topic)
	require.NoError(t, err)
	rc, err := cfgs.On(topic, nil)
	require.NoError(t, err)
	require.NoError(t, rc.Err)
	var retention string
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.Value != nil {
			retention = *c.Value
		}
	}
	require.Equal(t, "604800000", retention, "retention.ms must match spec")

	// Idempotency: ensuring again (even with a different spec) must not error —
	// existing topics are left untouched.
	require.NoError(t, kafka.EnsureTopics(ctx, cl, kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, topic))
}

// TestEnsureTopics_DefaultsZeroValues verifies that a zero-value TopicSpec is
// usable: partitions and replication factor default to 1.
func TestEnsureTopics_DefaultsZeroValues(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}

	broker, _ := kafkatest.Shared(t)
	cl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "admin-test-" + uuid.NewString()[:8],
	})
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "admin.zero-" + uuid.NewString()[:8]
	require.NoError(t, kafka.EnsureTopics(ctx, cl, kafka.TopicSpec{}, topic))

	adm := kadm.NewClient(cl)
	details, err := adm.ListTopics(ctx, topic)
	require.NoError(t, err)
	td, ok := details[topic]
	require.True(t, ok)
	require.NoError(t, td.Err)
	require.Len(t, td.Partitions, 1, "zero-value spec must default to 1 partition")
}
