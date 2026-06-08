package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// Config configures the HTTP server.
type Config struct {
	Addr              string        `env:"HTTP_ADDR" env-default:":8080"`
	ReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" env-default:"5s"`
	ReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" env-default:"15s"`
	WriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" env-default:"30s"`
	IdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" env-default:"60s"`
}

// Server wraps a chi router and an http.Server with the standard stack.
type Server struct {
	mux      *chi.Mux
	http     *http.Server
	serveErr chan error
	addr     string
}

// New builds a Server with the recover + request-id middleware preinstalled.
func New(cfg Config) *Server {
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

	mux := chi.NewRouter()
	mux.Use(RequestID)
	mux.Use(Recover)

	return &Server{
		mux:      mux,
		addr:     cfg.Addr,
		serveErr: make(chan error, 1),
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

// Start binds the listener and serves in a background goroutine.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", s.addr, err)
	}
	s.addr = ln.Addr().String()
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.serveErr <- err
		}
	}()
	return nil
}

// Notify returns a channel that receives a fatal Serve error (if the
// listener dies unexpectedly). It never receives on graceful shutdown.
func (s *Server) Notify() <-chan error { return s.serveErr }

// Addr returns the actual bound address (useful when Addr used :0).
func (s *Server) Addr() string { return s.addr }

// Shutdown gracefully drains in-flight requests within ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
