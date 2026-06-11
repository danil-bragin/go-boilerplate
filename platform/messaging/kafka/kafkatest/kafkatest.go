// Package kafkatest provides a test helper that starts a Redpanda container
// (Kafka-compatible broker + built-in Schema Registry) via testcontainers-go.
package kafkatest

import (
	"context"
	"fmt"
	"sync"
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

// Shared state: one lazily-started Redpanda container per test binary
// (i.e. per package). The singleton keeps container-heavy packages from
// paying a fresh broker startup (~5s) per test.
var (
	sharedOnce      sync.Once
	sharedContainer *redpanda.Container
	sharedBroker    string
	sharedSRURL     string
	errShared       error
)

// Shared returns the seed-broker address and Schema Registry URL of a
// package-shared Redpanda container, starting it on first use (sync.Once).
//
// Because the broker is shared by every test in the package, tests MUST use
// unique topic and consumer-group names (e.g. a uuid suffix) to stay isolated.
//
// The container is NOT terminated via t.Cleanup; pair Shared with a TestMain
// that calls TerminateShared after m.Run:
//
//	func TestMain(m *testing.M) {
//		code := m.Run()
//		kafkatest.TerminateShared()
//		os.Exit(code)
//	}
func Shared(t *testing.T) (broker string, srURL string) {
	t.Helper()

	sharedOnce.Do(func() {
		ctx := context.Background()

		container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
		if err != nil {
			errShared = fmt.Errorf("start redpanda: %w", err)
			return
		}
		sharedContainer = container

		if sharedBroker, err = container.KafkaSeedBroker(ctx); err != nil {
			errShared = fmt.Errorf("get kafka seed broker: %w", err)
			return
		}
		if sharedSRURL, err = container.SchemaRegistryAddress(ctx); err != nil {
			errShared = fmt.Errorf("get schema registry address: %w", err)
		}
	})
	if errShared != nil {
		t.Fatalf("kafkatest: shared redpanda: %v", errShared)
	}
	return sharedBroker, sharedSRURL
}

// TerminateShared stops the shared Redpanda container if one was started.
// Call it from TestMain after m.Run() (safe to call when no test used Shared;
// the testcontainers reaper is the fallback if a TestMain is missing).
func TerminateShared() {
	if sharedContainer != nil {
		_ = sharedContainer.Terminate(context.Background())
		sharedContainer = nil
	}
}

// SASLCreds are the SCRAM-SHA-256 superuser credentials provisioned on a
// SASL-enabled Redpanda started by NewRedpandaSASL.
type SASLCreds struct {
	User string
	Pass string
}

// NewRedpandaSASL starts a Redpanda container with SASL/SCRAM-SHA-256
// authentication enabled and a single superuser service account, returning the
// seed-broker address and the provisioned credentials.
//
// The container is dedicated to the calling test (terminated via t.Cleanup) —
// SASL changes the broker's auth surface, so it is intentionally NOT folded
// into the package-shared plaintext container. Use it to prove the franz-go
// SASL handshake end-to-end.
func NewRedpandaSASL(t *testing.T) (broker string, creds SASLCreds) {
	t.Helper()

	ctx := context.Background()

	creds = SASLCreds{User: "superuser", Pass: "superuser-secret"}

	container, err := redpanda.Run(
		ctx,
		"redpandadata/redpanda:v24.2.7",
		redpanda.WithEnableSASL(),
		redpanda.WithEnableKafkaAuthorization(),
		redpanda.WithNewServiceAccount(creds.User, creds.Pass),
		redpanda.WithSuperusers(creds.User),
	)
	if err != nil {
		t.Fatalf("kafkatest: start SASL redpanda: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("kafkatest: terminate SASL redpanda: %v", err)
		}
	})

	broker, err = container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("kafkatest: get kafka seed broker (SASL): %v", err)
	}

	return broker, creds
}
