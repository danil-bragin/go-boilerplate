package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates the given topics if they do not already exist.
// "Topic already exists" errors are silently ignored; any other per-topic
// error causes EnsureTopics to return a combined error.
func EnsureTopics(ctx context.Context, cl *kgo.Client, partitions int32, replication int16, topics ...string) error {
	adm := kadm.NewClient(cl)
	responses, err := adm.CreateTopics(ctx, partitions, replication, nil, topics...)
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
