package httpserver_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/web/httpserver"
)

// BenchmarkMiddlewareChain measures the full RequestID → AccessLog → Recover
// stack around a trivial handler. The logger writes to io.Discard so I/O
// does not dominate. This exercises allocation and lock overhead of the chain.
func BenchmarkMiddlewareChain(b *testing.B) {
	logger, sync := log.New(log.Config{Level: "info", Format: "json"}, io.Discard)
	defer sync() //nolint:errcheck

	trivial := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := httpserver.RequestID(
		httpserver.AccessLog(
			httpserver.Recover(trivial),
		),
	)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, "/bench", http.NoBody)
		req = req.WithContext(log.Into(req.Context(), logger))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}
