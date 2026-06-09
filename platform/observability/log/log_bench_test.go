package log_test

import (
	"io"
	"testing"

	"go-boilerplate/platform/observability/log"
)

func BenchmarkLogInfo(b *testing.B) {
	logger, _ := log.New(log.Config{Level: "info", Format: "json"}, io.Discard)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark message", "iter", i, "key", "value")
	}
}
