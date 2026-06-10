# Foundation (Sub-project 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the reusable `platform/` foundation packages (config, logging, lifecycle, telemetry, HTTP I/O, health, HTTP server) plus a runnable skeleton service that wires them together and shuts down gracefully.

**Architecture:** Monorepo, single Go module `go-boilerplate`, Go 1.25. Zero business logic — only reusable platform building blocks. Manual constructor DI; a `run.Closer` registers teardown callbacks run in reverse order on SIGTERM. Logging is stdlib `log/slog` with a zap backend via `zapslog`. HTTP edge is stdlib `net/http` + chi middleware. Errors returned to clients are RFC7807 problem+json. Everything is TDD: failing test first, minimal impl, green, commit.

**Tech Stack:** Go 1.25 · `log/slog` + `go.uber.org/zap` + `go.uber.org/zap/exp/zapslog` · `github.com/ilyakaznacheev/cleanenv` · `github.com/go-chi/chi/v5` · `github.com/go-playground/validator/v10` · OpenTelemetry (`go.opentelemetry.io/otel` + OTLP) · `github.com/stretchr/testify` · `go.uber.org/goleak` · Taskfile · golangci-lint.

**Out of scope (later sub-projects):** Postgres/sqlc/outbox (SP2), Kafka/serde (SP3), CQRS behaviors (SP4), cache/blob/resilience/auth/authz/audit/featureflags (SP5), example services (SP6), compose/CI/deploy (SP7).

---

## File Structure

Created in this sub-project:

```
go-boilerplate/
├── go.mod                              module go-boilerplate, go 1.25
├── go.sum
├── Taskfile.yml                        dev tasks
├── .gitignore
├── .golangci.yml                       lint config
├── platform/
│   ├── config/
│   │   ├── config.go                   Load[T] helper over cleanenv
│   │   └── config_test.go
│   ├── log/
│   │   ├── log.go                      slog+zapslog setup, level parsing
│   │   ├── context.go                  ctx logger + trace-id enrichment
│   │   └── log_test.go
│   ├── run/
│   │   ├── closer.go                   reverse-order teardown registry
│   │   ├── run.go                      signal handling, two-phase shutdown
│   │   ├── closer_test.go
│   │   └── run_test.go
│   ├── telemetry/
│   │   ├── telemetry.go                OTel tracer+meter providers + shutdown
│   │   └── telemetry_test.go
│   ├── httpx/
│   │   ├── problem.go                  RFC7807 problem+json
│   │   ├── decode.go                   JSON decode + validate
│   │   ├── respond.go                  JSON success responses
│   │   ├── problem_test.go
│   │   └── decode_test.go
│   ├── health/
│   │   ├── health.go                   liveness/readiness aggregator + handlers
│   │   └── health_test.go
│   └── httpserver/
│       ├── server.go                   chi server builder + graceful stop
│       ├── middleware.go               recover, request-id, slog access log
│       ├── server_test.go
│       └── middleware_test.go
└── cmd/
    └── skeleton/
        └── main.go                     wires everything; runnable demo
```

Each platform package has ONE clear responsibility and depends only on stdlib + its declared third-party libs. No platform package imports `cmd/` or any example.

---

## Task 0: Repository scaffold

**Files:**
- Create: `go.mod`, `.gitignore`, `.golangci.yml`, `Taskfile.yml`

- [ ] **Step 1: Initialize the module**

Run:
```bash
cd /Users/npden4ik/Projects/go-boilerplate
go mod init go-boilerplate
go mod edit -go=1.25
```
Expected: `go.mod` created with `module go-boilerplate` and `go 1.25`.

- [ ] **Step 2: Create `.gitignore`**

```gitignore
# Binaries
/bin/
/tmp/
*.exe
*.out

# Test / coverage
*.test
coverage.out
coverage.html

# Env
.env
.env.local

# Editor
.idea/
.vscode/
.DS_Store
```

- [ ] **Step 3: Create `.golangci.yml`**

```yaml
run:
  timeout: 5m
  go: "1.25"

linters:
  enable:
    - errcheck
    - govet
    - ineffassign
    - staticcheck
    - unused
    - gofmt
    - goimports
    - revive
    - bodyclose
    - contextcheck
    - errorlint
    - noctx

issues:
  exclude-rules:
    - path: _test\.go
      linters:
        - errcheck
```

- [ ] **Step 4: Create `Taskfile.yml`**

```yaml
version: "3"

tasks:
  tidy:
    desc: Sync go.mod/go.sum
    cmds:
      - go mod tidy

  test:
    desc: Run all tests with race detector
    cmds:
      - go test -race ./...

  lint:
    desc: Run golangci-lint
    cmds:
      - golangci-lint run ./...

  build:
    desc: Build the skeleton binary
    cmds:
      - go build -o bin/skeleton ./cmd/skeleton

  run:
    desc: Run the skeleton service
    cmds:
      - go run ./cmd/skeleton
```

- [ ] **Step 5: Verify the module builds (empty)**

Run: `go build ./...`
Expected: success, no output (no packages yet).

- [ ] **Step 6: Commit**

```bash
git init
git add go.mod .gitignore .golangci.yml Taskfile.yml
git commit -m "chore: scaffold go module, lint config, taskfile"
```

---

## Task 1: `platform/config` — typed config loader over cleanenv

**Files:**
- Create: `platform/config/config.go`
- Test: `platform/config/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/config"
)

type sampleConfig struct {
	Port     int    `env:"PORT" env-default:"8080"`
	LogLevel string `env:"LOG_LEVEL" env-default:"info"`
	DSN      string `env:"DSN" env-required:"true"`
}

func TestLoad_UsesDefaultsAndEnv(t *testing.T) {
	t.Setenv("DSN", "postgres://localhost/db")
	t.Setenv("PORT", "9090")

	cfg, err := config.Load[sampleConfig]()
	require.NoError(t, err)
	require.Equal(t, 9090, cfg.Port)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, "postgres://localhost/db", cfg.DSN)
}

func TestLoad_MissingRequiredFails(t *testing.T) {
	os.Unsetenv("DSN")
	_, err := config.Load[sampleConfig]()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./platform/config/...`
Expected: FAIL — package `go-boilerplate/platform/config` does not exist.

- [ ] **Step 3: Write minimal implementation**

`platform/config/config.go`:
```go
// Package config loads strongly-typed configuration from environment
// variables (and optional config files) using struct tags.
package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

// Load reads configuration of type T from environment variables.
// Struct fields use cleanenv tags: `env:"NAME"`, `env-default:"x"`,
// `env-required:"true"`, `env-separator:","`.
func Load[T any]() (T, error) {
	var cfg T
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return cfg, fmt.Errorf("config: read env: %w", err)
	}
	return cfg, nil
}

// LoadFromFile reads configuration of type T from a config file
// (YAML/JSON/TOML/ENV by extension) and then overlays environment variables.
func LoadFromFile[T any](path string) (T, error) {
	var cfg T
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return cfg, fmt.Errorf("config: read %s: %w", path, err)
	}
	return cfg, nil
}
```

- [ ] **Step 4: Add dependency and run test**

Run:
```bash
go get github.com/ilyakaznacheev/cleanenv@latest
go get github.com/stretchr/testify@latest
go test ./platform/config/...
```
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add platform/config go.mod go.sum
git commit -m "feat(config): typed env config loader over cleanenv"
```

---

## Task 2: `platform/log` — slog with zap backend + context enrichment

**Files:**
- Create: `platform/log/log.go`, `platform/log/context.go`
- Test: `platform/log/log_test.go`

- [ ] **Step 1: Write the failing test**

```go
package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/log"
)

func TestNew_WritesStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(log.Config{Level: "debug", Format: "json"}, &buf)

	logger.Info("hello", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "hello", entry["msg"])
	require.Equal(t, "value", entry["key"])
}

func TestParseLevel(t *testing.T) {
	require.Equal(t, "WARN", log.ParseLevel("warn").String())
	require.Equal(t, "INFO", log.ParseLevel("nonsense").String()) // fallback
}

func TestContextLogger_RoundTrips(t *testing.T) {
	var buf bytes.Buffer
	base := log.New(log.Config{Level: "info", Format: "json"}, &buf)

	ctx := log.Into(context.Background(), base.With("svc", "orders"))
	log.From(ctx).Info("msg")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "orders", entry["svc"])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./platform/log/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/log/log.go`**

```go
// Package log provides structured logging built on log/slog with a
// zap backend (via zapslog) for high-throughput services.
package log

import (
	"io"
	"log/slog"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

// Config controls logger construction.
type Config struct {
	Level  string `env:"LOG_LEVEL" env-default:"info"`  // debug|info|warn|error
	Format string `env:"LOG_FORMAT" env-default:"json"` // json|console
}

// ParseLevel maps a level string to slog.Level, defaulting to Info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New builds a *slog.Logger backed by zap writing to w.
func New(cfg Config, w io.Writer) *slog.Logger {
	level := zapLevel(ParseLevel(cfg.Level))

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "time"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	if strings.EqualFold(cfg.Format, "console") {
		encoder = zapcore.NewConsoleEncoder(encCfg)
	} else {
		encoder = zapcore.NewJSONEncoder(encCfg)
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(w), level)
	handler := zapslog.NewHandler(core, zapslog.WithCaller(false))
	return slog.New(handler)
}

func zapLevel(l slog.Level) zapcore.Level {
	switch {
	case l <= slog.LevelDebug:
		return zapcore.DebugLevel
	case l < slog.LevelWarn:
		return zapcore.InfoLevel
	case l < slog.LevelError:
		return zapcore.WarnLevel
	default:
		return zapcore.ErrorLevel
	}
}
```

- [ ] **Step 4: Write `platform/log/context.go`**

```go
package log

import (
	"context"
	"log/slog"
)

type ctxKey struct{}

// Into returns a copy of ctx carrying the given logger.
func Into(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// From returns the logger stored in ctx, or slog.Default() if none.
func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
```

- [ ] **Step 5: Add deps and run tests**

Run:
```bash
go get go.uber.org/zap@latest
go test ./platform/log/...
```
Expected: PASS (all three tests).

- [ ] **Step 6: Commit**

```bash
git add platform/log go.mod go.sum
git commit -m "feat(log): slog logger with zap backend and context helpers"
```

---

## Task 3: `platform/run` — Closer + signal-driven two-phase shutdown

**Files:**
- Create: `platform/run/closer.go`, `platform/run/run.go`
- Test: `platform/run/closer_test.go`, `platform/run/run_test.go`

- [ ] **Step 1: Write the failing Closer test**

`platform/run/closer_test.go`:
```go
package run_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/run"
)

func TestCloser_RunsInReverseOrder(t *testing.T) {
	var order []string
	c := run.NewCloser()
	c.Add("a", func(context.Context) error { order = append(order, "a"); return nil })
	c.Add("b", func(context.Context) error { order = append(order, "b"); return nil })
	c.Add("c", func(context.Context) error { order = append(order, "c"); return nil })

	require.NoError(t, c.Close(context.Background()))
	require.Equal(t, []string{"c", "b", "a"}, order)
}

func TestCloser_AggregatesErrorsButRunsAll(t *testing.T) {
	var ran int
	c := run.NewCloser()
	c.Add("ok1", func(context.Context) error { ran++; return nil })
	c.Add("bad", func(context.Context) error { ran++; return errors.New("boom") })
	c.Add("ok2", func(context.Context) error { ran++; return nil })

	err := c.Close(context.Background())
	require.Error(t, err)
	require.Equal(t, 3, ran) // all teardowns ran despite the error
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./platform/run/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/run/closer.go`**

```go
// Package run manages application lifecycle: ordered resource teardown
// and signal-driven graceful shutdown.
package run

import (
	"context"
	"errors"
	"sync"
)

// TeardownFunc releases a resource. It should respect ctx cancellation.
type TeardownFunc func(ctx context.Context) error

type teardown struct {
	name string
	fn   TeardownFunc
}

// Closer collects teardown callbacks and runs them in reverse registration
// order (last registered closes first), aggregating any errors.
type Closer struct {
	mu    sync.Mutex
	items []teardown
}

// NewCloser returns an empty Closer.
func NewCloser() *Closer { return &Closer{} }

// Add registers a named teardown callback.
func (c *Closer) Add(name string, fn TeardownFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, teardown{name: name, fn: fn})
}

// Close runs every teardown in reverse order. All callbacks run even if some
// fail; the returned error joins all failures (with their resource names).
func (c *Closer) Close(ctx context.Context) error {
	c.mu.Lock()
	items := c.items
	c.items = nil
	c.mu.Unlock()

	var errs []error
	for i := len(items) - 1; i >= 0; i-- {
		if err := items[i].fn(ctx); err != nil {
			errs = append(errs, errClose{name: items[i].name, err: err})
		}
	}
	return errors.Join(errs...)
}

type errClose struct {
	name string
	err  error
}

func (e errClose) Error() string { return e.name + ": " + e.err.Error() }
func (e errClose) Unwrap() error { return e.err }
```

- [ ] **Step 4: Run to verify Closer tests pass**

Run: `go test ./platform/run/...`
Expected: PASS.

- [ ] **Step 5: Write the failing run test**

`platform/run/run_test.go`:
```go
package run_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/run"
)

func TestRun_ReturnsWhenContextCanceled(t *testing.T) {
	c := run.NewCloser()
	var closed atomic.Bool
	c.Add("res", func(context.Context) error { closed.Store(true); return nil })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := run.Run(ctx, run.Options{ShutdownTimeout: time.Second}, c)
	require.NoError(t, err)
	require.True(t, closed.Load(), "closer must run on shutdown")
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `go test ./platform/run/... -run TestRun`
Expected: FAIL — `run.Run` / `run.Options` undefined.

- [ ] **Step 7: Write `platform/run/run.go`**

```go
package run

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Options configures Run.
type Options struct {
	ShutdownTimeout time.Duration // max time for Closer to finish
}

// Run blocks until ctx is canceled or an OS termination signal (SIGINT/SIGTERM)
// arrives, then closes resources via closer within ShutdownTimeout.
func Run(ctx context.Context, opts Options, closer *Closer) error {
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 15 * time.Second
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	stop() // restore default signal handling so a second signal force-quits

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	return closer.Close(shutdownCtx)
}

// ensure os import is used even if signal pkg changes; harmless guard.
var _ = os.Interrupt
```

- [ ] **Step 8: Run all run-package tests**

Run: `go test -race ./platform/run/...`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add platform/run
git commit -m "feat(run): reverse-order Closer and signal-driven graceful shutdown"
```

---

## Task 4: `platform/telemetry` — OTel tracer + meter providers

**Files:**
- Create: `platform/telemetry/telemetry.go`
- Test: `platform/telemetry/telemetry_test.go`

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./platform/telemetry/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/telemetry/telemetry.go`**

```go
// Package telemetry configures OpenTelemetry tracer and meter providers.
// When Enabled is false, providers are installed but no exporter is wired,
// so spans/metrics are cheap no-ops — convenient for tests and local runs.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config controls telemetry setup.
type Config struct {
	ServiceName  string `env:"OTEL_SERVICE_NAME" env-default:"service"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" env-default:"localhost:4317"`
	Enabled      bool   `env:"OTEL_ENABLED" env-default:"false"`
}

// ShutdownFunc flushes and stops telemetry providers.
type ShutdownFunc func(ctx context.Context) error

// Setup installs global tracer/meter providers and W3C propagation.
// It returns a shutdown function that flushes exporters.
func Setup(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))

	if cfg.Enabled {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(ctx)
	}, nil
}
```

- [ ] **Step 4: Add deps and run test**

Run:
```bash
go get go.opentelemetry.io/otel@latest
go get go.opentelemetry.io/otel/sdk@latest
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@latest
go test ./platform/telemetry/...
```
Expected: PASS.

> Note: if the `semconv/v1.26.0` import path is unavailable in the resolved otel version, run `go doc go.opentelemetry.io/otel/semconv` to find the bundled version directory and update the import to match. The `semconv.ServiceName` helper exists in all recent versions.

- [ ] **Step 5: Commit**

```bash
git add platform/telemetry go.mod go.sum
git commit -m "feat(telemetry): OpenTelemetry tracer provider with OTLP exporter and shutdown"
```

---

## Task 5: `platform/httpx` — problem+json, JSON decode/validate, respond

**Files:**
- Create: `platform/httpx/problem.go`, `platform/httpx/decode.go`, `platform/httpx/respond.go`
- Test: `platform/httpx/problem_test.go`, `platform/httpx/decode_test.go`

- [ ] **Step 1: Write the failing problem test**

`platform/httpx/problem_test.go`:
```go
package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpx"
)

func TestWriteProblem_SetsStatusAndContentType(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteProblem(rec, httpx.Problem{
		Status: http.StatusNotFound,
		Title:  "Not Found",
		Detail: "order 42 not found",
	})

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, float64(404), body["status"])
	require.Equal(t, "order 42 not found", body["detail"])
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./platform/httpx/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/httpx/problem.go`**

```go
// Package httpx provides JSON request decoding+validation and RFC7807
// problem+json error responses for HTTP handlers.
package httpx

import (
	"encoding/json"
	"net/http"
)

// Problem is an RFC7807 problem detail.
type Problem struct {
	Type   string `json:"type,omitempty"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	// Errors carries field-level validation messages (optional extension).
	Errors map[string]string `json:"errors,omitempty"`
}

// WriteProblem writes p as application/problem+json with p.Status.
func WriteProblem(w http.ResponseWriter, p Problem) {
	if p.Status == 0 {
		p.Status = http.StatusInternalServerError
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// Error writes a minimal problem for the given status and detail.
func Error(w http.ResponseWriter, status int, detail string) {
	WriteProblem(w, Problem{Status: status, Detail: detail})
}
```

- [ ] **Step 4: Write the failing decode test**

`platform/httpx/decode_test.go`:
```go
package httpx_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpx"
)

type createReq struct {
	Name  string `json:"name" validate:"required"`
	Email string `json:"email" validate:"required,email"`
}

func TestDecode_ValidPayload(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	got, err := httpx.Decode[createReq](r)
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}

func TestDecode_InvalidPayloadReturnsValidationError(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"","email":"nope"}`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)

	var ve *httpx.ValidationError
	require.ErrorAs(t, err, &ve)
	require.Contains(t, ve.Fields, "Name")
	require.Contains(t, ve.Fields, "Email")
}

func TestDecode_MalformedJSON(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}
```

- [ ] **Step 5: Run to verify it fails**

Run: `go test ./platform/httpx/... -run TestDecode`
Expected: FAIL — `httpx.Decode` / `httpx.ValidationError` undefined.

- [ ] **Step 6: Write `platform/httpx/decode.go`**

```go
package httpx

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-playground/validator/v10"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

// ValidationError reports field-level validation failures.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %v", e.Fields)
}

// Decode reads a JSON body of type T from r and validates it via struct tags.
// It rejects unknown fields. Returns *ValidationError on validation failure.
func Decode[T any](r *http.Request) (T, error) {
	var v T
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, fmt.Errorf("httpx: decode json: %w", err)
	}

	if err := validate.Struct(v); err != nil {
		var verrs validator.ValidationErrors
		if ok := asValidationErrors(err, &verrs); ok {
			fields := make(map[string]string, len(verrs))
			for _, fe := range verrs {
				fields[fe.Field()] = fmt.Sprintf("failed on '%s'", fe.Tag())
			}
			return v, &ValidationError{Fields: fields}
		}
		return v, fmt.Errorf("httpx: validate: %w", err)
	}
	return v, nil
}

func asValidationErrors(err error, target *validator.ValidationErrors) bool {
	if verrs, ok := err.(validator.ValidationErrors); ok {
		*target = verrs
		return true
	}
	return false
}

// WriteDecodeError maps a decode/validation error to an appropriate problem.
func WriteDecodeError(w http.ResponseWriter, err error) {
	var ve *ValidationError
	if asValidation(err, &ve) {
		WriteProblem(w, Problem{
			Status: http.StatusUnprocessableEntity,
			Title:  "Validation Failed",
			Errors: ve.Fields,
		})
		return
	}
	Error(w, http.StatusBadRequest, "invalid request body")
}

func asValidation(err error, target **ValidationError) bool {
	for err != nil {
		if ve, ok := err.(*ValidationError); ok {
			*target = ve
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
```

- [ ] **Step 7: Write `platform/httpx/respond.go`**

```go
package httpx

import (
	"encoding/json"
	"net/http"
)

// JSON writes v as application/json with the given status.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}
```

- [ ] **Step 8: Add dep and run all httpx tests**

Run:
```bash
go get github.com/go-playground/validator/v10@latest
go test ./platform/httpx/...
```
Expected: PASS (all tests).

- [ ] **Step 9: Commit**

```bash
git add platform/httpx go.mod go.sum
git commit -m "feat(httpx): problem+json, JSON decode/validate, respond helpers"
```

---

## Task 6: `platform/health` — liveness/readiness aggregator

**Files:**
- Create: `platform/health/health.go`
- Test: `platform/health/health_test.go`

- [ ] **Step 1: Write the failing test**

```go
package health_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/health"
)

func TestReadyz_AllPassReturns200(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code)
}

func TestReadyz_OneFailureReturns503(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return errors.New("down") })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
}

func TestLivez_AlwaysOK_UnlessNotLive(t *testing.T) {
	h := health.New()

	rec := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 200, rec.Code)

	h.SetNotLive() // shutdown flips liveness
	rec2 := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec2, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 503, rec2.Code)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./platform/health/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/health/health.go`**

```go
// Package health provides Kubernetes-style liveness (/livez) and readiness
// (/readyz) endpoints. Liveness reflects process health only; readiness
// aggregates dependency checks and is flipped to "not ready" on shutdown.
package health

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Check probes a dependency; returns nil when healthy.
type Check func(ctx context.Context) error

// Health aggregates liveness and readiness state.
type Health struct {
	live  atomic.Bool
	ready atomic.Bool

	mu     sync.RWMutex
	checks map[string]Check
}

// New returns a Health that starts live and ready with no checks.
func New() *Health {
	h := &Health{checks: make(map[string]Check)}
	h.live.Store(true)
	h.ready.Store(true)
	return h
}

// AddReadiness registers a named readiness check.
func (h *Health) AddReadiness(name string, c Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = c
}

// SetNotLive marks the process as not live (e.g. fatal internal error).
func (h *Health) SetNotLive() { h.live.Store(false) }

// SetNotReady marks the service as not ready (call at the start of shutdown).
func (h *Health) SetNotReady() { h.ready.Store(false) }

// LivezHandler serves liveness.
func (h *Health) LivezHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if h.live.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
}

// ReadyzHandler runs all readiness checks; 200 only if ready and all pass.
func (h *Health) ReadyzHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		h.mu.RLock()
		checks := make(map[string]Check, len(h.checks))
		for k, v := range h.checks {
			checks[k] = v
		}
		h.mu.RUnlock()

		for _, c := range checks {
			if err := c(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./platform/health/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/health
git commit -m "feat(health): livez/readiness aggregator with shutdown gating"
```

---

## Task 7: `platform/httpserver` — chi server + middleware + graceful stop

**Files:**
- Create: `platform/httpserver/server.go`, `platform/httpserver/middleware.go`
- Test: `platform/httpserver/server_test.go`, `platform/httpserver/middleware_test.go`

- [ ] **Step 1: Write the failing middleware test**

`platform/httpserver/middleware_test.go`:
```go
package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpserver"
)

func TestRequestID_AddsHeaderAndContext(t *testing.T) {
	var seen string
	h := httpserver.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFromContext(r.Context())
		w.WriteHeader(200)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	require.NotEmpty(t, seen)
	require.Equal(t, seen, rec.Header().Get("X-Request-Id"))
}

func TestRecover_TurnsPanicInto500Problem(t *testing.T) {
	h := httpserver.Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	require.Equal(t, 500, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./platform/httpserver/...`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `platform/httpserver/middleware.go`**

```go
// Package httpserver builds a configured chi HTTP server with a standard
// middleware stack and graceful shutdown.
package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"go-boilerplate/platform/httpx"
	"go-boilerplate/platform/log"
)

type ctxKey int

const requestIDKey ctxKey = iota

const requestIDHeader = "X-Request-Id"

// RequestID ensures every request has an ID, echoed in the response header
// and stored in the context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = newID()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFromContext returns the request ID, or "" if absent.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Recover converts a panic in a downstream handler into a 500 problem+json.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.From(r.Context()).Error("panic recovered",
					"panic", rec, "path", r.URL.Path)
				httpx.Error(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
```

- [ ] **Step 4: Run middleware tests**

Run: `go test ./platform/httpserver/... -run 'TestRequestID|TestRecover'`
Expected: PASS.

- [ ] **Step 5: Write the failing server test**

`platform/httpserver/server_test.go`:
```go
package httpserver_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpserver"
)

func TestServer_ServesAndShutsDown(t *testing.T) {
	srv := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0"})
	srv.Mux().Get("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	require.NoError(t, srv.Start())
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + srv.Addr() + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, "pong", string(body))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `go test ./platform/httpserver/... -run TestServer`
Expected: FAIL — `httpserver.New` / `Config` / `Mux` / `Start` / `Addr` / `Shutdown` undefined.

- [ ] **Step 7: Write `platform/httpserver/server.go`**

```go
package httpserver

import (
	"context"
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
	mux  *chi.Mux
	http *http.Server
	ln   net.Listener
	addr string
}

// New builds a Server with the recover + request-id middleware preinstalled.
func New(cfg Config) *Server {
	mux := chi.NewRouter()
	mux.Use(RequestID)
	mux.Use(Recover)

	return &Server{
		mux:  mux,
		addr: cfg.Addr,
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
	s.ln = ln
	s.addr = ln.Addr().String()
	go func() { _ = s.http.Serve(ln) }()
	return nil
}

// Addr returns the actual bound address (useful when Addr used :0).
func (s *Server) Addr() string { return s.addr }

// Shutdown gracefully drains in-flight requests within ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
```

- [ ] **Step 8: Add dep and run all httpserver tests with race + leak check**

Run:
```bash
go get github.com/go-chi/chi/v5@latest
go test -race ./platform/httpserver/...
```
Expected: PASS (all tests).

- [ ] **Step 9: Commit**

```bash
git add platform/httpserver go.mod go.sum
git commit -m "feat(httpserver): chi server with recover/request-id middleware and graceful shutdown"
```

---

## Task 8: `cmd/skeleton` — wire everything into a runnable service

**Files:**
- Create: `cmd/skeleton/main.go`
- Test: `cmd/skeleton/main_test.go`

- [ ] **Step 1: Write the failing smoke test**

`cmd/skeleton/main_test.go`:
```go
package main

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestApp_StartHealthAndStop(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("go.opentelemetry.io/otel/sdk/trace.(*batchSpanProcessor).processQueue"),
	)

	app, err := newApp(context.Background())
	require.NoError(t, err)
	require.NoError(t, app.start())

	resp, err := http.Get("http://" + app.server.Addr() + "/livez")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", string(body))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, app.stop(ctx))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./cmd/skeleton/...`
Expected: FAIL — `newApp` undefined.

- [ ] **Step 3: Write `cmd/skeleton/main.go`**

```go
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
	closer.Add("telemetry", shutdownTel)

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
	ctx := context.Background()

	a, err := newApp(ctx)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	if err := a.start(); err != nil {
		slog.Error("start failed", "error", err)
		os.Exit(1)
	}

	// Block until SIGINT/SIGTERM, then close resources (reverse order).
	if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.closer); err != nil {
		a.logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	a.logger.Info("shutdown complete")
}
```

- [ ] **Step 4: Add dep and run the smoke test**

Run:
```bash
go get go.uber.org/goleak@latest
go test -race ./cmd/skeleton/...
```
Expected: PASS.

> Note: if goleak flags an OTLP/otel background goroutine, the test already ignores the batch span processor. Telemetry is disabled by default (`OTEL_ENABLED=false`), so no exporter goroutine should start; the ignore is defensive.

- [ ] **Step 5: Run the whole suite + tidy + lint**

Run:
```bash
go mod tidy
go test -race ./...
golangci-lint run ./... || true
```
Expected: all tests PASS. (Lint output reviewed; fix any reported issues.)

- [ ] **Step 6: Manually verify the binary runs and shuts down**

Run:
```bash
go run ./cmd/skeleton &
sleep 1
curl -s localhost:8080/livez   # expect: ok
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/readyz  # expect: 200
kill -TERM %1
wait
```
Expected: `livez` → `ok`, `readyz` → `200`, process logs "shutdown complete" and exits 0.

- [ ] **Step 7: Commit**

```bash
git add cmd go.mod go.sum
git commit -m "feat(skeleton): runnable service wiring config/log/telemetry/http/health/shutdown"
```

---

## Self-Review (completed)

**Spec coverage (against plan.md §3 platform packages, Foundation subset):**
- config ✅ Task 1 · log ✅ Task 2 · run/Closer + two-phase shutdown ✅ Task 3 · telemetry ✅ Task 4 · httpx (problem+json, decode/validate) ✅ Task 5 · health (/livez, /readyz) ✅ Task 6 · httpserver (chi, recover, request-id, graceful) ✅ Task 7 · wiring demonstrates manual DI + Closer ✅ Task 8.
- Deferred to later sub-projects (explicitly out of scope above): pg, kafka, serde, outbox, cqrs, cache, blob, resilience, auth, authz, audit, featureflags, examples, compose/CI.

**Type consistency:** `run.Closer.Add(name, TeardownFunc)` and `Closer.Close(ctx)` used identically in Tasks 3 and 8. `httpserver.New(Config)`, `.Mux()`, `.Start()`, `.Addr()`, `.Shutdown(ctx)` consistent across Tasks 7–8. `health.New()`, `.LivezHandler()`, `.ReadyzHandler()`, `.SetNotReady()` consistent Tasks 6–8. `log.New(Config, io.Writer)`, `log.From(ctx)` consistent Tasks 2, 7, 8. `telemetry.Setup(ctx, Config) (ShutdownFunc, error)` consistent Tasks 4, 8.

**Placeholder scan:** No TBD/TODO; every code step contains complete, compilable Go. Two `> Note` callouts give explicit fallback instructions (semconv version path; goleak ignore) rather than leaving gaps.

**Known follow-ups for SP2+:** slog/OTel trace-id correlation middleware (needs telemetry span context — add an access-log middleware in httpserver once tracing middleware exists); access logging middleware deferred until OTel HTTP instrumentation lands in SP5 cross-cutting.
