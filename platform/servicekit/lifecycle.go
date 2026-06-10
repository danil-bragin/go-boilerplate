package servicekit

import (
	"context"
	"fmt"
	"time"
)

// Start starts the admin HTTP server and all registered consumer/relay/cleaner
// goroutines. Non-blocking.
//
// Admin server bind failure is FATAL by default: without /livez, /readyz and
// /metrics the pod is a half-alive blind spot — orchestrators cannot probe it
// and operators cannot see it. ADMIN_BIND_OPTIONAL=true restores the old
// warn-and-continue behavior for setups that accept that risk. Tests use
// AdminAddr "127.0.0.1:0" (ephemeral port), so they are unaffected.
//
// Ordering guarantee: a runCtx is created and cancelRun is registered as the
// LAST entry in the Closer (so it fires FIRST in LIFO teardown). This means
// goroutines receive context cancellation before the pg pool and kafka client
// are closed.
func (s *Service) Start() error {
	if err := s.adminServer.Start(); err != nil {
		if !s.cfg.AdminBindOptional {
			return fmt.Errorf("servicekit: admin server failed to start on %s "+
				"(set ADMIN_BIND_OPTIONAL=true to tolerate): %w", s.adminServer.Addr(), err)
		}
		s.logger.Warn("admin server failed to start — continuing without /livez,/readyz,/metrics (ADMIN_BIND_OPTIONAL=true)",
			"error", err, "addr", s.adminServer.Addr())
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
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			fn(runCtx)
		}()
	}

	// Register cancelRun near the end → fires early in LIFO teardown (right
	// after the drain-gate and the public HTTP servers), stopping goroutines
	// before the pg pool and kafka client are released. WAITS for the
	// goroutines to actually exit: a consumer's final detached offset commit
	// must complete before the next closer entry closes the kgo client.
	s.closer.Add("consumers-cancel", func(ctx context.Context) error {
		cancelRun()
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("servicekit: consumers did not stop before teardown budget: %w", ctx.Err())
		}
	})

	// Public HTTP servers (AddHTTPServer): bind failure is fatal — same
	// contract as the admin server above (no half-alive pods). Each server's
	// shutdown closer is registered IMMEDIATELY after its successful Start:
	// when server N fails to bind, servers 0..N-1 already have closers, so
	// the teardown Main runs after the failed Start shuts them down instead
	// of leaking live listeners.
	//
	// The registrations land between consumers-cancel (above) and the
	// drain-gate (below), so in LIFO teardown each public server drains its
	// in-flight requests AFTER /readyz flipped to 503 (no new traffic routed)
	// and BEFORE consumers are cancelled and kafka/pg close (handlers may
	// still use the producer/pool while draining).
	for _, ns := range s.httpServers {
		if err := ns.srv.Start(); err != nil {
			cancelRun()
			return fmt.Errorf("servicekit: http server %q failed to start on %s: %w",
				ns.name, ns.srv.Addr(), err)
		}
		srv, name := ns.srv, ns.name
		s.closer.Add("http-server-"+name, func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		})
		s.wg.Add(1)
		go s.watchServe(name, srv.Notify())
		s.logger.Info("http server started", "server", name, "addr", srv.Addr())
	}

	// Drain-gate is registered LAST → fires FIRST in LIFO teardown, BEFORE
	// consumers are cancelled and before any server shuts down: flip /readyz
	// to 503, then hold for DRAIN_GRACE so load balancers observe the
	// not-ready state and stop routing traffic. The admin server (registered
	// first in New, so closed last) keeps serving the 503 for the whole drain.
	s.closer.Add("drain-gate", func(ctx context.Context) error {
		s.h.SetNotReady()
		if s.cfg.DrainGrace <= 0 {
			return nil
		}
		timer := time.NewTimer(s.cfg.DrainGrace)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		return nil
	})

	s.logger.Info("service started", "admin_addr", s.adminServer.Addr())
	return nil
}

// watchServe watches one public HTTP server for a fatal serve error after a
// successful Start (the Notify channel reports a listener that died; it is
// closed without an error on graceful shutdown). On a fatal error it:
//
//  1. logs the failure,
//  2. flips readiness to 503 so load balancers stop routing immediately,
//  3. forwards the error to the Fatal channel so servicekit.Main tears the
//     whole process down and exits non-zero.
//
// Step 3 is the one that actually recovers the pod: liveness stays green and
// readiness only removes the pod from endpoints, so without a process exit
// the pod would sit NotReady forever. The caller must s.wg.Add(1) first; the
// watcher exits on runCtx cancellation during normal teardown.
func (s *Service) watchServe(name string, notify <-chan error) {
	defer s.wg.Done()
	select {
	case err := <-notify:
		if err != nil {
			s.logger.Error("http server failed", "server", name, "error", err)
			s.h.SetNotReady()
			select {
			case s.fatal <- err:
			default: // a fatal error is already pending; one is enough
			}
		}
	case <-s.runCtx.Done():
	}
}

// Stop closes all resources via the Closer. Teardown order (see package doc):
// drain-gate (readyz→503 + DRAIN_GRACE) → consumers-cancel → service closers →
// kafka/pg/telemetry/log → admin server last.
//
// Stop intentionally does NOT cancel runCtx up front: the consumers-cancel
// closer does that AFTER the drain-gate has held the grace window, so
// consumers keep processing while the load balancer drains traffic.
func (s *Service) Stop(ctx context.Context) error {
	return s.closer.Close(ctx)
}
