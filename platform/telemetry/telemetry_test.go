package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

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
