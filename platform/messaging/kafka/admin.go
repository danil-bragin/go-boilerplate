package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicSpec describes how topics created by EnsureTopics are provisioned.
//
// Partitions bounds consumer-group parallelism: a topic with P partitions is
// consumed by at most P members of a group. ReplicationFactor is the broker
// replica count per partition — use ≥3 in production clusters (1 is only
// acceptable for local single-broker development). Configs holds raw topic
// configs (e.g. "retention.ms") applied at creation time.
//
// Zero values default to 1 partition / RF 1 so the zero TopicSpec stays
// usable in tests.
type TopicSpec struct {
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// EnsureTopics creates the given topics with the provided spec if they do not
// already exist. "Topic already exists" errors are silently ignored — the
// spec is NOT reconciled against existing topics (partition count, RF, and
// configs of pre-existing topics are left untouched; treat production topic
// management as IaC and use EnsureTopics for dev/test bootstrap only).
// Any other per-topic error causes EnsureTopics to return a combined error.
func EnsureTopics(ctx context.Context, cl *kgo.Client, spec TopicSpec, topics ...string) error {
	partitions := spec.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	replication := spec.ReplicationFactor
	if replication <= 0 {
		replication = 1
	}
	var configs map[string]*string
	if len(spec.Configs) > 0 {
		configs = make(map[string]*string, len(spec.Configs))
		for k, v := range spec.Configs {
			configs[k] = kadm.StringPtr(v)
		}
	}

	adm := kadm.NewClient(cl)
	responses, err := adm.CreateTopics(ctx, partitions, replication, configs, topics...)
	if err != nil {
		return fmt.Errorf("kafka: create topics request: %w", err)
	}

	var errs []error
	for _, resp := range responses {
		if resp.Err == nil {
			continue
		}
		if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			continue
		}
		errs = append(errs, fmt.Errorf("topic %s: %w", resp.Topic, resp.Err))
	}
	return errors.Join(errs...)
}
