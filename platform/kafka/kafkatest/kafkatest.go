// Package kafkatest provides a test helper that starts a Redpanda container
// (Kafka-compatible broker + built-in Schema Registry) via testcontainers-go.
package kafkatest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// NewRedpanda starts a Redpanda container and returns:
//   - broker: the Kafka seed-broker address ("host:port")
//   - srURL:  the Schema Registry HTTP base URL ("http://host:port")
//
// The container is automatically terminated when the test (or sub-test)
// finishes via t.Cleanup.
func NewRedpanda(t *testing.T) (broker string, srURL string) {
	t.Helper()

	ctx := context.Background()

	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatalf("kafkatest: start redpanda: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("kafkatest: terminate redpanda: %v", err)
		}
	})

	broker, err = container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("kafkatest: get kafka seed broker: %v", err)
	}

	srURL, err = container.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("kafkatest: get schema registry address: %v", err)
	}

	return broker, srURL
}
