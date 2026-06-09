package servicekit

import (
	"context"
)

// Start starts the admin HTTP server and all registered consumer/relay/cleaner
// goroutines. Non-blocking.
//
// Admin server start failure is treated as a warning (logged, not returned) since
// the admin endpoint is observability-only and must not prevent service startup.
// This is important in tests where multiple services share the same default port.
//
// Ordering guarantee: a runCtx is created and cancelRun is registered as the
// LAST entry in the Closer (so it fires FIRST in LIFO teardown). This means
// goroutines receive context cancellation before the pg pool and kafka client
// are closed.
func (s *Service) Start() error {
	if err := s.adminServer.Start(); err != nil {
		// Non-fatal: log and continue. The admin endpoint is observability-only;
		// a port-bind failure (e.g. during tests with multiple services sharing
		// the default port) must not prevent the service from consuming messages.
		s.logger.Warn("admin server failed to start", "error", err, "addr", s.adminServer.Addr())
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	s.runCtx = runCtx
	s.cancelRun = cancelRun

	// Launch inbox cleanup if configured (interval > 0). All services that use
	// the harness migrate the inbox table, so this is safe to launch
	// unconditionally. Future services with no inbox table will simply log a
	// harmless DELETE error; set InboxCleanupInterval=0 to suppress it.
	s.startInboxCleanup(runCtx)

	for _, g := range s.goroutines {
		fn := g // capture
		go fn(runCtx)
	}

	// Register cancelRun LAST → fires FIRST in LIFO closer, stopping goroutines
	// before the pg pool and kafka client are released.
	s.closer.Add("consumers-cancel", func(context.Context) error {
		cancelRun()
		return nil
	})

	s.logger.Info("service started", "admin_addr", s.adminServer.Addr())
	return nil
}

// Stop cancels consumer goroutines and closes all resources via the Closer.
func (s *Service) Stop(ctx context.Context) error {
	if s.cancelRun != nil {
		s.cancelRun()
	}
	return s.closer.Close(ctx)
}
