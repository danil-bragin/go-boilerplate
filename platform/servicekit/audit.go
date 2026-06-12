package servicekit

import (
	"context"
	"errors"

	"go-boilerplate/platform/security/audit"
)

// AddAuditWriter wires a BufferedAuditWriter's drain loop into the service
// lifecycle (started on Start, drained on shutdown). Build the writer with
// audit.NewBufferedAuditWriter(store, cfg) and pass it here, then use
// audit.AsyncAudit(writer, ...) in the Eventual-consistency command pipelines.
// Must be called before Start.
func (s *Service) AddAuditWriter(w *audit.BufferedAuditWriter) error {
	if s.started {
		return errors.New("servicekit: AddAuditWriter called after Start — the writer would never run")
	}
	s.goroutines = append(s.goroutines, func(ctx context.Context) {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("audit writer stopped unexpectedly", "error", err)
		}
	})
	return nil
}
