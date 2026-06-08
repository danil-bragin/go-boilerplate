package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"

	"go-boilerplate/platform/telemetry"
)

func BenchmarkSpanDisabled(b *testing.B) {
	_, _ = telemetry.Setup(context.Background(), telemetry.Config{ServiceName: "bench", Enabled: false})
	tr := otel.Tracer("bench")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tr.Start(ctx, "op")
		span.End()
	}
}
