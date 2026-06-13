package servicekit

// White-box tests for AddAuditDrain's per-physical-shard fan-out. They assert
// goroutine ACCOUNTING and guard conditions only — no DB is dialed:
// audit.NewPgStore does not touch the pool until a query runs, and pool.Writer()
// on a zero-value Pool returns a nil pgxpool that is never dereferenced at
// wiring time. The end-to-end "staged intents drain into the chain" path is
// covered by the audit package's DrainPending integration tests.

import (
	"log/slog"
	"testing"
	"time"

	"go-boilerplate/platform/storage/pg"
)

// countDrains builds a Service over m physical shards and returns how many
// background goroutines AddAuditDrain registers.
func countDrains(t *testing.T, m int, interval time.Duration) int {
	t.Helper()
	pools := make([]*pg.Pool, m)
	for i := range pools {
		pools[i] = &pg.Pool{} // never dereferenced before the loop runs
	}
	s := &Service{
		logger: slog.Default(),
		pool:   pools[0],
		shards: pg.WrapShards(pools),
	}
	if err := s.AddAuditDrain(interval, 0); err != nil {
		t.Fatalf("AddAuditDrain: %v", err)
	}
	return len(s.goroutines)
}

// TestAddAuditDrain_OnePerPhysicalShard: one single-active drain goroutine per
// physical shard — the chain must be applied serially per shard.
func TestAddAuditDrain_OnePerPhysicalShard(t *testing.T) {
	if got := countDrains(t, 1, time.Second); got != 1 {
		t.Fatalf("M=1: want 1 drain goroutine, got %d", got)
	}
	if got := countDrains(t, 3, time.Second); got != 3 {
		t.Fatalf("M=3: want 3 drain goroutines (one per physical shard), got %d", got)
	}
}

// TestAddAuditDrain_DisabledWhenIntervalNonPositive registers nothing.
func TestAddAuditDrain_DisabledWhenIntervalNonPositive(t *testing.T) {
	if got := countDrains(t, 2, 0); got != 0 {
		t.Fatalf("interval<=0 must register no goroutines, got %d", got)
	}
}

// TestAddAuditDrain_AfterStartErrors: late registration is a programming error.
func TestAddAuditDrain_AfterStartErrors(t *testing.T) {
	s := &Service{logger: slog.Default(), shards: pg.WrapShards([]*pg.Pool{{}}), started: true}
	if err := s.AddAuditDrain(time.Second, 0); err == nil {
		t.Fatal("AddAuditDrain after Start must error")
	}
}

// TestAddAuditDrain_RequiresPostgres: WithoutPG leaves shards nil.
func TestAddAuditDrain_RequiresPostgres(t *testing.T) {
	s := &Service{logger: slog.Default()}
	if err := s.AddAuditDrain(time.Second, 0); err == nil {
		t.Fatal("AddAuditDrain without postgres must error")
	}
}
