package cqrs

import (
	"context"
	"time"

	"go-boilerplate/platform/log"
)

// Logging returns a Behavior that logs handler invocations using the logger
// stored in ctx (via log.From). It logs at Debug on start and at Info on
// success or Error on failure — both with duration_ms. The command/result
// values are never logged (may contain secrets).
func Logging[C, R any](name string) Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			logger := log.From(ctx)
			logger.Debug("handler started", "handler", name)

			start := time.Now()
			res, err := next(ctx, cmd)
			dur := float64(time.Since(start).Milliseconds())

			if err != nil {
				logger.Error("handler failed",
					"handler", name,
					"status", "error",
					"duration_ms", dur,
					"error", err,
				)
			} else {
				logger.Info("handler succeeded",
					"handler", name,
					"status", "ok",
					"duration_ms", dur,
				)
			}
			return res, err
		}
	}
}
