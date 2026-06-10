// Command skeleton is a minimal runnable HTTP service built on the shared
// servicekit harness — the same wiring pattern every example service uses.
// It runs with NO Kafka and NO Postgres (servicekit.WithoutKafka /
// servicekit.WithoutPG): the harness still provides logger, telemetry,
// admin HTTP server (/livez /readyz /metrics), optional pyroscope profiling,
// and readiness-first graceful teardown; the skeleton adds a public HTTP
// server with demo routes that exercise the full middleware stack.
package main

import (
	"context"
	"io"
	"net/http"
	"time"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/observability/health"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/web/httpserver"
	"go-boilerplate/platform/web/httpx"
)

type appConfig struct {
	servicekit.Config
	HTTP httpserver.Config
}

type app struct {
	cfg    appConfig
	svc    *servicekit.Service
	server *httpserver.Server
	health *health.Health
}

func newApp(ctx context.Context) (*app, error) {
	cfg, err := config.Load[appConfig]()
	if err != nil {
		return nil, err
	}

	// The harness without Kafka and without Postgres: nothing is dialed,
	// only logger/telemetry/admin-server/lifecycle wiring remains.
	svc, err := servicekit.New(ctx, cfg.Config, nil, "",
		servicekit.WithoutKafka(), servicekit.WithoutPG())
	if err != nil {
		return nil, err
	}

	h := svc.Health()
	server := httpserver.New(cfg.HTTP)

	// Health endpoints on the PUBLIC server too (the admin server already
	// serves them): handy for single-port deployments and exercised by e2e.
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

	// The harness starts the server in svc.Start (bind failure fatal) and
	// shuts it down right after the drain-gate during teardown.
	svc.AddHTTPServer("public", server)

	return &app{cfg: cfg, svc: svc, server: server, health: h}, nil
}

// Start launches the harness (admin + public servers). Non-blocking.
func (a *app) Start() error {
	if err := a.svc.Start(); err != nil {
		return err
	}

	// A1: emit "service stopping" at the START of shutdown; registered after
	// svc.Start so it executes before even the drain-gate in LIFO teardown.
	a.svc.Closer().Add("shutdown-log", func(context.Context) error {
		a.svc.Logger().Info("service stopping")
		return nil
	})

	a.svc.Logger().Info("service started", "addr", a.server.Addr())
	return nil
}

// Stop closes all resources via the harness closer.
func (a *app) Stop(ctx context.Context) error {
	return a.svc.Stop(ctx)
}

// Closer exposes the harness closer for servicekit.Main / run.Run.
func (a *app) Closer() *run.Closer { return a.svc.Closer() }

func main() {
	servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
		return newApp(ctx)
	})
}
