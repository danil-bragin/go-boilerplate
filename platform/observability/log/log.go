// Package log provides structured logging built on log/slog with a
// zap backend (via zapslog) for high-throughput services.
//
// Trace correlation: when callers use the context-aware log methods
// (InfoContext, WarnContext, ErrorContext, DebugContext) and the context carries
// an active OTel span, the logger automatically appends trace_id and span_id
// attributes to every log record. This requires the OTel SDK to be installed
// (e.g. via platform/telemetry.Setup); when no span is present the fields are
// simply omitted.
package log

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

// Config controls logger construction.
type Config struct {
	Level  string `env:"LOG_LEVEL" envDefault:"info"`  // debug|info|warn|error
	Format string `env:"LOG_FORMAT" envDefault:"json"` // json|console
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

// Option customises New beyond the env-driven Config.
type Option func(*options)

type options struct {
	otelProvider otellog.LoggerProvider
}

// WithOTelBridge fans every log record out to the given OTel LoggerProvider
// (the otelslog bridge) IN ADDITION to the stdout sink — stdout stays the
// primary sink and keeps working when the provider's exporter is down. The
// bridge passes the record context to the SDK, so records emitted inside a
// span carry the trace/span IDs natively in OTLP (independent of the
// trace_id/span_id attributes added for the stdout JSON). A nil provider is
// ignored, which lets callers wire telemetry.Providers.LoggerProvider through
// unconditionally.
func WithOTelBridge(p otellog.LoggerProvider) Option {
	return func(o *options) { o.otelProvider = p }
}

// otelScopeName is the instrumentation scope for the otelslog bridge.
const otelScopeName = "go-boilerplate/platform/observability/log"

// New builds a *slog.Logger backed by zap writing to w. The returned sync
// function flushes any buffered log data and should be called on shutdown
// (e.g. registered with run.Closer). Sync errors on os.Stdout/os.Stderr
// (EINVAL/ENOTTY on some platforms) are common and can be ignored by callers.
// New panics immediately if w is nil.
//
// With WithOTelBridge the logger fans out to stdout AND the OTel provider;
// Config.Level gates both sinks. OTLP flushing is owned by the provider's
// Shutdown (telemetry), not by the returned sync function.
func New(cfg Config, w io.Writer, opts ...Option) (*slog.Logger, func() error) {
	if w == nil {
		panic("log: New: writer must not be nil")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}

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
	zapHandler := zapslog.NewHandler(core, zapslog.WithCaller(false))
	stdout := slog.Handler(&traceHandler{Handler: zapHandler})

	if o.otelProvider == nil {
		return slog.New(stdout), core.Sync
	}

	bridge := otelslog.NewHandler(otelScopeName, otelslog.WithLoggerProvider(o.otelProvider))
	return slog.New(&fanoutHandler{
		handlers: []slog.Handler{stdout, bridge},
		min:      ParseLevel(cfg.Level),
	}), core.Sync
}

// fanoutHandler dispatches every record to all child handlers. The configured
// minimum level gates the fan-out as a whole (the zap core would filter the
// stdout path anyway; the bridge has no level of its own). Child errors are
// joined, and one failing sink never stops the others.
type fanoutHandler struct {
	handlers []slog.Handler
	min      slog.Level
}

func (h *fanoutHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.min
}

func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, child := range h.handlers {
		if !child.Enabled(ctx, r.Level) {
			continue
		}
		if err := child.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	children := make([]slog.Handler, len(h.handlers))
	for i, child := range h.handlers {
		children[i] = child.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: children, min: h.min}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	children := make([]slog.Handler, len(h.handlers))
	for i, child := range h.handlers {
		children[i] = child.WithGroup(name)
	}
	return &fanoutHandler{handlers: children, min: h.min}
}

// traceHandler wraps a slog.Handler and injects trace_id and span_id
// attributes into every log record when the context carries a valid OTel span.
// It only adds the fields when callers use context-aware log methods
// (InfoContext, WarnContext, etc.) because those are the only paths that supply
// a non-background context to Handle.
type traceHandler struct {
	slog.Handler
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs returns a new traceHandler whose inner handler has the given attrs.
func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

// WithGroup returns a new traceHandler whose inner handler is grouped.
func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
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
