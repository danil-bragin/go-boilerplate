package gateway_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMain terminates the package-shared Redpanda and Postgres containers
// (started lazily by kafkatest.Shared / pgtest.SharedDSN) after all tests
// have run. Sharing one broker + one Postgres server across the package cuts
// the per-test container startup cost; isolation comes from a fresh database
// per test (pgtest.SharedDSN) and unique topic names per test
// (configureTopics).
func TestMain(m *testing.M) {
	// No load balancer is watching /readyz in tests: the 5s DRAIN_GRACE
	// default would add a dead 5s to every app Stop (~20 app starts in this
	// package). Drain ORDER is still covered by servicekit's lifecycle tests.
	os.Setenv("DRAIN_GRACE", "0")

	code := m.Run()
	kafkatest.TerminateShared()
	pgtest.TerminateShared()
	os.Exit(code)
}

// Per-test unique topic names on the shared broker. configureTopics
// regenerates them and points the gateway at them via the GATEWAY_*_TOPIC
// envs; the consumer group ("gateway-projection") can stay fixed because
// offsets are tracked per (group, topic). Tests in this package run
// sequentially (no t.Parallel), so package-level variables are safe.
var (
	topicCommands       string
	topicOrdersEvents   string
	topicPaymentsEvents string
)

// configureTopics gives the current test its own command/event topics so
// records and committed offsets never leak between tests on the shared
// broker. Call it before gateway.NewApp / NewProjectionApp (startApp and
// startAppWithVerifier already do).
func configureTopics(t *testing.T) {
	t.Helper()
	suffix := "-" + uuid.NewString()[:8]
	topicCommands = "orders.commands" + suffix
	topicOrdersEvents = "orders.events" + suffix
	topicPaymentsEvents = "payments.events" + suffix
	t.Setenv("GATEWAY_COMMANDS_TOPIC", topicCommands)
	t.Setenv("GATEWAY_ORDERS_EVENTS_TOPIC", topicOrdersEvents)
	t.Setenv("GATEWAY_PAYMENTS_EVENTS_TOPIC", topicPaymentsEvents)

	// The shared broker lives for the whole package run while each test
	// creates its own 6-partition topics (+DLTs). Delete them afterwards:
	// Redpanda caps the partition count by available memory, and ~30 leaked
	// partitions per test would exhaust that cap mid-package (CreateTopics
	// then fails with INVALID_PARTITIONS). Cleanup runs after the app's own
	// cleanup (LIFO), i.e. after the consumers are stopped.
	broker, _ := kafkatest.Shared(t)
	names := []string{
		topicCommands, topicOrdersEvents, topicPaymentsEvents,
		topicCommands + ".DLT", topicOrdersEvents + ".DLT", topicPaymentsEvents + ".DLT",
	}
	t.Cleanup(func() {
		cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
		if err != nil {
			return // best-effort cleanup only
		}
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = kadm.NewClient(cl).DeleteTopics(ctx, names...)
	})
}
