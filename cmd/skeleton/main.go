// Command skeleton is a minimal runnable service that wires the platform
// foundation packages together: config, logging, telemetry, HTTP server,
// health endpoints, and graceful shutdown via the Closer.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/health"
	"go-boilerplate/platform/httpserver"
	"go-boilerplate/platform/log"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/telemetry"
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

	logger := log.New(cfg.Log, os.Stdout)
	slog.SetDefault(logger)

	closer := run.NewCloser()

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

	closer.Add("http-server", func(ctx context.Context) error {
		h.SetNotReady() // stop accepting traffic before draining
		return server.Shutdown(ctx)
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
	a.logger.Info("service stopping")
	return a.closer.Close(ctx)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := newApp(ctx)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	if err := a.start(); err != nil {
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
		a.logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	a.logger.Info("shutdown complete")
}
