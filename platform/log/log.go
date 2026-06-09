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
	"io"
	"log/slog"
	"strings"

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

// New builds a *slog.Logger backed by zap writing to w. The returned sync
// function flushes any buffered log data and should be called on shutdown
// (e.g. registered with run.Closer). Sync errors on os.Stdout/os.Stderr
// (EINVAL/ENOTTY on some platforms) are common and can be ignored by callers.
// New panics immediately if w is nil.
func New(cfg Config, w io.Writer) (*slog.Logger, func() error) {
	if w == nil {
		panic("log: New: writer must not be nil")
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
	return slog.New(&traceHandler{Handler: zapHandler}), core.Sync
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
