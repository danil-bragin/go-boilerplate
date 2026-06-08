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
