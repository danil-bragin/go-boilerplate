package servicekit

// White-box test for the inbox-cleanup worker lifecycle. Runs in -short
// mode: the cleanup loop only touches the pool on its first tick, so a long
// interval plus a zero-value pool exercises the goroutine accounting without
// any container.

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// TestRegisterInboxCleanup_TrackedByWaitGroup asserts the teardown invariant:
// the inbox-cleanup worker must be registered as a harness goroutine and
// therefore tracked by s.wg when Start launches it — the consumers-cancel
// closer waits on s.wg BEFORE the pg closer runs, so an untracked cleanup
// goroutine could race its DELETE against pool teardown.
func TestRegisterInboxCleanup_TrackedByWaitGroup(t *testing.T) {
	pool := &pg.Pool{} // never dereferenced before the first tick
	s := &Service{
		cfg: Config{
			InboxCleanupInterval: time.Hour, // first tick far in the future
			InboxRetention:       time.Hour,
		},
		logger: slog.Default(),
		pool:   pool,
		shards: pg.WrapPool(pool),
	}

	s.registerInboxCleanup()
	if len(s.goroutines) != 1 {
		t.Fatalf("registerInboxCleanup must register exactly one goroutine, got %d", len(s.goroutines))
	}

	// Launch it the way Start does: wg-tracked with the run context.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn := s.goroutines[0]
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn(runCtx)
	}()

	waited := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waited)
	}()

	// The cleanup goroutine is alive (blocked on its ticker), so a tracked
	// goroutine keeps wg.Wait blocked. An untracked one lets it return now.
	select {
	case <-waited:
		t.Fatal("s.wg.Wait returned while the cleanup goroutine is still running — the goroutine is not wg-tracked")
	case <-time.After(100 * time.Millisecond):
	}

	// Cancelling runCtx (what the consumers-cancel closer does) must release
	// the goroutine and therefore wg.Wait — before the pg closer would run.
	cancel()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup goroutine did not exit after runCtx cancellation")
	}
}

// TestRegisterInboxCleanup_DisabledWhenIntervalZero asserts that a zero
// interval (or missing pool) registers nothing.
func TestRegisterInboxCleanup_DisabledWhenIntervalZero(t *testing.T) {
	pool := &pg.Pool{}
	s := &Service{
		cfg:    Config{InboxCleanupInterval: 0},
		logger: slog.Default(),
		pool:   pool,
		shards: pg.WrapPool(pool),
	}
	s.registerInboxCleanup()
	if len(s.goroutines) != 0 {
		t.Fatalf("interval=0 must register no goroutine, got %d", len(s.goroutines))
	}

	s2 := &Service{
		cfg:    Config{InboxCleanupInterval: time.Hour},
		logger: slog.Default(),
		pool:   nil, // WithoutPG
	}
	s2.registerInboxCleanup()
	if len(s2.goroutines) != 0 {
		t.Fatalf("nil pool must register no goroutine, got %d", len(s2.goroutines))
	}
}
