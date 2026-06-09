package httpserver_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/web/httpserver"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// startServer builds, starts, and registers cleanup for a Server bound to a
// random port. Routes must be mounted via the mutate callback BEFORE Start.
func startServer(t *testing.T, mutate func(*httpserver.Server), opts ...httpserver.ServerOption) *httpserver.Server {
	t.Helper()
	srv := httpserver.New(httpserver.Config{
		Addr:           "127.0.0.1:0",
		HandlerTimeout: 100 * time.Millisecond,
		MaxBodyBytes:   16,
	}, opts...)
	mutate(srv)
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

// TestWithoutTimeout_GroupsControlTheirOwnDeadline: a server built with
// WithoutTimeout does not apply the global TimeoutHandler — a group that opts
// back in via httpserver.Timeout gets 503, the streaming group does not.
func TestWithoutTimeout_GroupsControlTheirOwnDeadline(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(250 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	srv := startServer(t, func(s *httpserver.Server) {
		// JSON group: opts back into the request timeout.
		s.Mux().Group(func(r chi.Router) {
			r.Use(httpserver.Timeout(50 * time.Millisecond))
			r.Get("/json", slow)
		})
		// Streaming group: no TimeoutHandler → slow handler completes.
		s.Mux().Group(func(r chi.Router) {
			r.Get("/stream", slow)
		})
	}, httpserver.WithoutTimeout())

	base := "http://" + srv.Addr()

	respJSON, err := http.Get(base + "/json") //nolint:noctx
	require.NoError(t, err)
	defer respJSON.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, respJSON.StatusCode,
		"group with Timeout middleware must time out")

	respStream, err := http.Get(base + "/stream") //nolint:noctx
	require.NoError(t, err)
	defer respStream.Body.Close()
	require.Equal(t, http.StatusOK, respStream.StatusCode,
		"group without Timeout must not be cut off by the server-wide handler timeout")
}

// TestWithoutMaxBytes_GroupsControlTheirOwnBodyLimit: WithoutMaxBytes removes
// the global cap; a group can install its own larger MaxBytes.
func TestWithoutMaxBytes_GroupsControlTheirOwnBodyLimit(t *testing.T) {
	echoLen := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := startServer(t, func(s *httpserver.Server) {
		// Upload group: 64-byte budget (server default in this test is 16).
		s.Mux().Group(func(r chi.Router) {
			r.Use(httpserver.MaxBytes(64))
			r.Post("/upload", echoLen)
		})
		// Strict group: keeps a tiny limit.
		s.Mux().Group(func(r chi.Router) {
			r.Use(httpserver.MaxBytes(8))
			r.Post("/strict", echoLen)
		})
	}, httpserver.WithoutMaxBytes(), httpserver.WithoutTimeout())

	base := "http://" + srv.Addr()
	payload := strings.Repeat("a", 32) // 32 bytes: > strict(8), > old global (16), < upload(64)

	respUp, err := http.Post(base+"/upload", "text/plain", strings.NewReader(payload)) //nolint:noctx
	require.NoError(t, err)
	defer respUp.Body.Close()
	require.Equal(t, http.StatusOK, respUp.StatusCode,
		"upload group must accept bodies above the old server-wide cap")

	respStrict, err := http.Post(base+"/strict", "text/plain", strings.NewReader(payload)) //nolint:noctx
	require.NoError(t, err)
	defer respStrict.Body.Close()
	require.Equal(t, http.StatusRequestEntityTooLarge, respStrict.StatusCode,
		"strict group must reject bodies above its own cap")
}

// TestDefaultStack_StillAppliesTimeoutAndMaxBytes: without options the global
// Timeout/MaxBytes still protect every route (backward compatibility).
func TestDefaultStack_StillAppliesTimeoutAndMaxBytes(t *testing.T) {
	srv := startServer(t, func(s *httpserver.Server) {
		s.Mux().Get("/slow", func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(250 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		})
	})

	resp, err := http.Get("http://" + srv.Addr() + "/slow") //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
