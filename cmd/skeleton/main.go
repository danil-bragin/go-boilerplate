// Command skeleton is a minimal runnable service that wires the platform
// foundation packages together: config, logging, telemetry, HTTP server,
// health endpoints, and graceful shutdown via the Closer.
package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/health"
	"go-boilerplate/platform/httpserver"
	"go-boilerplate/platform/httpx"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"

	// automaxprocs sets GOMAXPROCS to match the container CPU quota at startup.
	// Go 1.25+ also does this natively when GOMAXPROCS is unset, but automaxprocs
	// is the belt-and-suspenders standard and works across all supported versions.
	_ "go.uber.org/automaxprocs"
)

type appConfig struct {
	Log       log.Config
	Telemetry telemetry.Config
	HTTP      httpserver.Config
}

type app struct {
	cfg    appConfig
	logger *slog.Logger
	server *httpserver.Server
	health *health.Health
	closer *run.Closer
}

func newApp(ctx context.Context) (*app, error) {
	cfg, err := config.Load[appConfig]()
	if err != nil {
		return nil, err
	}

	logger, logSync := log.New(cfg.Log, os.Stdout)
	slog.SetDefault(logger)

	closer := run.NewCloser()
	// log-sync is registered first so it runs last in the reverse-order
	// shutdown sequence — buffered logs are flushed after every other
	// component has finished logging its own shutdown messages.
	closer.Add("log-sync", func(context.Context) error {
		_ = logSync() // stdout sync errors are benign; ignore
		return nil
	})

	shutdownTel, err := telemetry.Setup(ctx, cfg.Telemetry)
	if err != nil {
		return nil, err
	}
	closer.Add("telemetry", func(ctx context.Context) error {
		return shutdownTel(ctx)
	})

	h := health.New()
	server := httpserver.New(cfg.HTTP)
	server.Mux().Method("GET", "/livez", h.LivezHandler())
	server.Mux().Method("GET", "/readyz", h.ReadyzHandler())

	// Demo routes – exercise the full middleware stack in e2e tests.
	server.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"pong": "true"})
	})
	server.Mux().Get("/boom", func(_ http.ResponseWriter, _ *http.Request) {
		panic("boom in handler")
	})
	server.Mux().Get("/slow", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		httpx.JSON(w, http.StatusOK, map[string]string{"slow": "done"})
	})
	server.Mux().Post("/echo", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "too big")
			return
		}
		httpx.JSON(w, http.StatusOK, map[string]int{"n": len(b)})
	})

	// Trivial passing readiness check so /readyz exercises the check path.
	h.AddReadiness("self", func(context.Context) error { return nil })

	closer.Add("http-server", func(ctx context.Context) error {
		h.SetNotReady() // stop accepting traffic before draining
		return server.Shutdown(ctx)
	})

	// A1: emit "service stopping" at the START of shutdown; registered last so
	// it executes first in the reverse-order teardown sequence.
	closer.Add("shutdown-log", func(context.Context) error {
		logger.Info("service stopping")
		return nil
	})

	return &app{cfg: cfg, logger: logger, server: server, health: h, closer: closer}, nil
}

func (a *app) start() error {
	if err := a.server.Start(); err != nil {
		return err
	}
	a.logger.Info("service started", "addr", a.server.Addr())
	return nil
}

func (a *app) stop(ctx context.Context) error {
	return a.closer.Close(ctx)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	a, err := newApp(ctx)
	if err != nil {
		cancel()
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	if err := a.start(); err != nil {
		cancel()
		slog.Error("start failed", "error", err)
		os.Exit(1)
	}

	// A fatal serve error (listener dies unexpectedly) triggers graceful
	// shutdown by canceling the run context.
	go func() {
		if serveErr := <-a.server.Notify(); serveErr != nil {
			a.logger.Error("http server failed", "error", serveErr)
			cancel()
		}
	}()

	// Block until SIGINT/SIGTERM or a fatal serve error, then close
	// resources (reverse order).
	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.closer); err != nil {
		cancel()
		a.logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	cancel()
	a.logger.Info("shutdown complete")
}
