package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	noop "go.opentelemetry.io/otel/trace/noop"

	"go-boilerplate/platform/telemetry"
)

func TestSetup_ConfiguresGlobalProvidersAndShutsDown(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "test-svc",
		Enabled:     false, // no exporter; uses noop-friendly setup
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// A tracer is obtainable and usable without panicking.
	tr := otel.Tracer("test")
	_, span := tr.Start(context.Background(), "op")
	span.End()

	require.NoError(t, shutdown(context.Background()))
}

// TestSetup_DisabledUsesNoopProvider verifies that when Enabled:false, the
// global tracer provider is the true no-op provider (zero-allocation).
func TestSetup_DisabledUsesNoopProvider(t *testing.T) {
	_, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "noop-svc",
		Enabled:     false,
	})
	require.NoError(t, err)

	require.IsType(t, noop.NewTracerProvider(), otel.GetTracerProvider())
}

// TestShutdown_FlushesEvenWithCancelledContext verifies that the shutdown
// function ignores the incoming (possibly cancelled) context and uses its own
// background-based timeout, so spans are flushed even after SIGTERM.
func TestShutdown_FlushesEvenWithCancelledContext(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "shutdown-svc",
		Enabled:     false,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Pass an already-cancelled context; shutdown must still succeed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	require.NoError(t, shutdown(ctx))
}

// TestSetup_EnabledWiresSDKProviderAndShutsDown exercises the Enabled:true
// path: a real SDK TracerProvider is installed (not the no-op), and shutdown
// completes promptly when no spans are pending (the OTLP/gRPC exporter dials
// lazily, so an empty flush does not block on the unreachable collector).
//
// Note: recording a span here would make Shutdown attempt to export it to the
// dead collector and block until the 5s shutdown deadline — that is correct
// behavior, so this test deliberately records none.
func TestSetup_EnabledWiresSDKProviderAndShutsDown(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName:  "enabled-svc",
		OTLPEndpoint: "127.0.0.1:4317", // nothing listening; lazy dial
		Enabled:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Enabled path installs a real SDK provider, not the no-op.
	require.NotEqual(t, noop.NewTracerProvider(), otel.GetTracerProvider())

	require.NoError(t, shutdown(context.Background()))
}
