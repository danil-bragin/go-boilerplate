package servicekit

// White-box test for the inbox-cleanup goroutine lifecycle. Runs in -short
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

// TestStartInboxCleanup_TrackedByWaitGroup asserts the teardown invariant:
// the inbox-cleanup goroutine must be tracked by s.wg, because the
// consumers-cancel closer waits on s.wg BEFORE the pg closer runs — an
// untracked cleanup goroutine could race its DELETE against pool teardown.
func TestStartInboxCleanup_TrackedByWaitGroup(t *testing.T) {
	s := &Service{
		cfg: Config{
			InboxCleanupInterval: time.Hour, // first tick far in the future
			InboxRetention:       time.Hour,
		},
		logger: slog.Default(),
		pool:   &pg.Pool{}, // never dereferenced before the first tick
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startInboxCleanup(runCtx)

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
