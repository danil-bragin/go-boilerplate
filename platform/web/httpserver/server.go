package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
)

// Config configures the HTTP server.
type Config struct {
	Addr              string        `env:"HTTP_ADDR"               envDefault:":8080"`
	ReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT"        envDefault:"15s"`
	WriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT"       envDefault:"30s"`
	IdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT"        envDefault:"60s"`
	// MaxBodyBytes caps the request body size (default 1 MiB).
	MaxBodyBytes int64 `env:"HTTP_MAX_BODY_BYTES" envDefault:"1048576"`
	// HandlerTimeout is the per-request timeout (default 30s).
	HandlerTimeout time.Duration `env:"HTTP_HANDLER_TIMEOUT" envDefault:"30s"`
}

// Server wraps a chi router and an http.Server with the standard stack.
type Server struct {
	mux      *chi.Mux
	http     *http.Server
	serveErr chan error
	mu       sync.Mutex
	closed   bool
	started  atomic.Bool
	addr     string

	// noMaxBytes records that the server-wide request-body cap was skipped
	// (WithoutMaxBytes); Start warns so a route group that forgot its own
	// MaxBytes does not silently accept unbounded bodies.
	noMaxBytes bool
}

// ServerOption customizes the middleware stack installed by New.
type ServerOption func(*serverOptions)

type serverOptions struct {
	noTimeout  bool
	noMaxBytes bool
}

// WithoutTimeout skips the server-wide Timeout (http.TimeoutHandler) so that
// route groups can manage their own deadlines. Use this when the server hosts
// streaming or large-transfer routes: TimeoutHandler buffers the entire
// response in memory, which is wrong for downloads/uploads. Re-apply the
// timeout per group via httpserver.Timeout:
//
//	srv := httpserver.New(cfg, httpserver.WithoutTimeout())
//	srv.Mux().Group(func(r chi.Router) {
//	    r.Use(httpserver.Timeout(30 * time.Second)) // JSON API group
//	    r.Get("/v1/orders/{id}", getOrder)
//	})
//	srv.Mux().Group(func(r chi.Router) {
//	    // streaming group: no TimeoutHandler — rely on the server's
//	    // Read/WriteTimeout and context deadlines instead.
//	    r.Get("/v1/orders/{id}/attachment/{name}", download)
//	})
func WithoutTimeout() ServerOption {
	return func(o *serverOptions) { o.noTimeout = true }
}

// WithoutMaxBytes skips the server-wide request-body cap so that route groups
// can set their own budgets via httpserver.MaxBytes — e.g. a small cap for
// JSON routes and a larger one for uploads:
//
//	srv := httpserver.New(cfg, httpserver.WithoutMaxBytes())
//	srv.Mux().Group(func(r chi.Router) {
//	    r.Use(httpserver.MaxBytes(1 << 20)) // JSON routes: 1 MiB
//	    ...
//	})
//	srv.Mux().Group(func(r chi.Router) {
//	    r.Use(httpserver.MaxBytes(64 << 20)) // uploads: 64 MiB
//	    ...
//	})
//
// CAUTION: a group without any MaxBytes has an unbounded request body.
// chi cannot introspect group middlewares, so this cannot be verified at
// wiring time — Start logs a WARN whenever the server-wide cap is absent as
// a reminder to audit every group.
func WithoutMaxBytes() ServerOption {
	return func(o *serverOptions) { o.noMaxBytes = true }
}

// New builds a Server with the standard middleware stack preinstalled.
//
// Middleware order (outermost → innermost):
//
//	SecurityHeaders → RequestID → OTel → AccessLog → Recover → MaxBytes → Timeout → RouteTag
//
// SecurityHeaders is outermost so defensive headers are set on every response,
// including error responses produced by Recover.
// RequestID ensures an id is in context before AccessLog reads it.
// AccessLog wraps with a capturingWriter to observe status+bytes; it sits
// outside Recover so it always logs the final status (200 or 500).
// Recover catches panics from handlers deeper in the chain; it installs its
// own capturingWriter to detect whether headers were already committed.
// MaxBytes + Timeout apply to the actual handler logic; both can be lifted to
// per-route-group control via WithoutMaxBytes / WithoutTimeout.
func New(cfg Config, opts ...ServerOption) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.ReadHeaderTimeout == 0 {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 15 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 60 * time.Second
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 1 << 20 // 1 MiB
	}
	if cfg.HandlerTimeout == 0 {
		cfg.HandlerTimeout = 30 * time.Second
	}

	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}

	mux := chi.NewRouter()
	mux.Use(SecurityHeaders)
	mux.Use(RequestID)
	mux.Use(OTel)
	mux.Use(AccessLog)
	mux.Use(Recover)
	if !o.noMaxBytes {
		mux.Use(MaxBytes(cfg.MaxBodyBytes))
	}
	if !o.noTimeout {
		mux.Use(Timeout(cfg.HandlerTimeout))
	}
	// RouteTag is INNERMOST: it reads the chi route pattern on the
	// request-serving goroutine (race-free even when TimeoutHandler abandons
	// a request) and publishes it to AccessLog via a guarded holder.
	mux.Use(RouteTag())

	return &Server{
		mux:        mux,
		addr:       cfg.Addr,
		noMaxBytes: o.noMaxBytes,
		serveErr:   make(chan error, 1),
		http: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
	}
}

// Mux exposes the router for route registration.
func (s *Server) Mux() *chi.Mux { return s.mux }

// OnShutdown registers fn to run when Shutdown begins (stdlib
// http.Server.RegisterOnShutdown). Use it to end long-lived connections that
// graceful drain would otherwise wait on for the whole teardown budget —
// SSE/streaming handlers should select on a channel that fn closes.
// Must be called before Start.
func (s *Server) OnShutdown(fn func()) { s.http.RegisterOnShutdown(fn) }

// Start binds the listener and serves in a background goroutine.
// It returns an error if called more than once.
func (s *Server) Start() error {
	// A6: guard against double-start.
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("httpserver: already started")
	}

	// Per-group MaxBytes middlewares are opaque to chi, so the server cannot
	// verify that every route group installed its own cap — the best feasible
	// signal is a startup WARN whenever the server-wide cap is absent.
	if s.noMaxBytes {
		slog.Warn("httpserver: started WithoutMaxBytes — no server-wide request-body cap; " +
			"every route group MUST install its own httpserver.MaxBytes or accept unbounded bodies")
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", s.addr, err)
	}
	s.addr = ln.Addr().String()
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// A3: send only while holding the lock and only if the channel has
			// not been closed yet — this eliminates the send-on-closed-channel
			// race that would otherwise occur if Shutdown() races this goroutine.
			s.mu.Lock()
			if !s.closed {
				select {
				case s.serveErr <- err:
				default:
				}
			}
			s.mu.Unlock()
		}
	}()
	return nil
}

// Notify returns a channel that receives a fatal Serve error (if the listener
// dies unexpectedly). The channel is CLOSED on graceful shutdown so callers
// that read from it (range / receive-with-ok) always unblock.
func (s *Server) Notify() <-chan error { return s.serveErr }

// Addr returns the actual bound address (useful when Addr used :0).
func (s *Server) Addr() string { return s.addr }

// Shutdown gracefully drains in-flight requests within ctx and then closes
// the Notify() channel so any consumer unblocks.
//
// It is safe to call Shutdown concurrently or more than once; the channel is
// closed exactly once under the lock so no send-on-closed race is possible.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	// A3: close exactly once under the lock. Setting closed=true before the
	// close() call means any concurrent send goroutine that acquires the lock
	// afterwards will see closed==true and skip the send, preventing a
	// send-on-closed-channel panic.
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.serveErr)
	}
	s.mu.Unlock()
	return err
}
