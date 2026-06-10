package kafka_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/kafka/kafkatest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestTransact_ConcurrentProducePromises_RaceFree exercises the window where
// asynchronous produce-promise callbacks (which append to the shared
// produce-error slice) run concurrently with the commit decision at the end
// of a batch. Run under -race: an unguarded read of the error slice in the
// commit path is a data race against in-flight promise callbacks.
//
// Functional assertion: with fan-out ProcessFn (many outputs per input) all
// outputs are committed exactly once and visible to a read-committed reader.
func TestTransact_ConcurrentProducePromises_RaceFree(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (redpanda container)")
	}
	broker, _ := kafkatest.Shared(t)

	ctx := context.Background()
	const (
		nIn     = 20
		fanOut  = 50
		wantOut = nIn * fanOut
	)

	inTopic := "txn-race-in-" + uuid.NewString()[:8]
	outTopic := "txn-race-out-" + uuid.NewString()[:8]

	adminCl, err := kafka.NewClient(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-race-admin",
	})
	require.NoError(t, err)
	defer adminCl.Close()

	require.NoError(t, kafka.EnsureTopics(ctx, adminCl,
		kafka.TopicSpec{Partitions: 1, ReplicationFactor: 1}, inTopic, outTopic))

	prod := kafka.NewProducer(adminCl)
	for i := 0; i < nIn; i++ {
		require.NoError(t, prod.Produce(ctx, kafka.Record{
			Topic: inTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		}))
	}
	require.NoError(t, prod.Close(ctx))

	tc, err := kafka.NewTransactConsumer(
		kafka.Config{Brokers: []string{broker}, ClientID: "txn-race-tc"},
		uniqueName("txn-race-txn-id"),
		uniqueName("txn-race-group"),
		[]string{inTopic},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRun()

	go func() {
		_ = tc.Run(runCtx, func(_ context.Context, rec kafka.Record) ([]kafka.Record, error) {
			outs := make([]kafka.Record, 0, fanOut)
			for j := 0; j < fanOut; j++ {
				outs = append(outs, kafka.Record{
					Topic: outTopic,
					Key:   rec.Key,
					Value: []byte(fmt.Sprintf("%s-out-%d", rec.Value, j)),
				})
			}
			return outs, nil
		})
	}()

	// Count outputs with a read-committed consumer.
	reader, err := kafka.NewConsumer(kafka.Config{
		Brokers:  []string{broker},
		ClientID: "txn-race-reader",
		GroupID:  uniqueName("txn-race-reader-group"),
	}, []string{outTopic})
	require.NoError(t, err)

	var seen atomic.Int64
	readerCtx, cancelReader := context.WithTimeout(ctx, 90*time.Second)
	defer cancelReader()
	go func() {
		_ = reader.Run(readerCtx, func(_ context.Context, _ kafka.Record) error {
			seen.Add(1)
			return nil
		})
	}()

	require.Eventually(t, func() bool { return seen.Load() >= wantOut },
		80*time.Second, 100*time.Millisecond,
		"all %d fan-out records must be committed and visible (got %d)", wantOut, seen.Load())

	// Settle and assert no duplicates leaked past EOS.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int64(wantOut), seen.Load(), "exactly-once: no duplicate outputs")

	cancelRun()
	cancelReader()
	_ = tc.Close(ctx)
	_ = reader.Close(ctx)
}
