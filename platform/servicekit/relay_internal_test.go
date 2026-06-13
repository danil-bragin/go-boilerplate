package servicekit

// White-box tests for AddOutboxRelay's per-physical-shard fan-out (Task B4,
// ADR-0019). They assert goroutine ACCOUNTING only — no DB is dialed: the
// relay/cleaner constructors do not touch the pool until Run, and pool.Writer()
// on a zero-value Pool simply returns a nil pgxpool that is never dereferenced
// at wiring time. The end-to-end "row on shard 1 actually gets published" path
// is covered by the eventual multi-shard e2e (two real containers).

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"
)

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, outbox.Message) error { return nil }

// countRelays builds a Service over m physical shards and returns how many
// background goroutines AddOutboxRelay registers.
func countRelays(t *testing.T, m int, cfg outbox.RelayConfig) int {
	t.Helper()
	pools := make([]*pg.Pool, m)
	for i := range pools {
		pools[i] = &pg.Pool{} // never dereferenced before Run
	}
	s := &Service{
		logger: slog.Default(),
		pool:   pools[0],
		shards: pg.WrapShards(pools),
	}
	if err := s.AddOutboxRelay(noopPublisher{}, cfg); err != nil {
		t.Fatalf("AddOutboxRelay: %v", err)
	}
	return len(s.goroutines)
}

// TestAddOutboxRelay_OneShardUnchanged: M=1, Shards=1 ⇒ one relay + one cleaner.
func TestAddOutboxRelay_OneShardUnchanged(t *testing.T) {
	if got := countRelays(t, 1, outbox.RelayConfig{SingleActive: true, Shards: 1}); got != 2 {
		t.Fatalf("M=1/relayShards=1: want 2 goroutines (relay+cleaner), got %d", got)
	}
}

// TestAddOutboxRelay_PerPhysicalShard: M physical shards × (R relays + 1
// cleaner) each. With M=3, R=2 that is 3*(2+1) = 9 goroutines — the critical
// fix: a relay fleet runs on EVERY physical shard, not just shard 0.
func TestAddOutboxRelay_PerPhysicalShard(t *testing.T) {
	const m, relayShards = 3, 2
	want := m * (relayShards + 1)
	if got := countRelays(t, m, outbox.RelayConfig{SingleActive: true, Shards: relayShards}); got != want {
		t.Fatalf("M=%d/relayShards=%d: want %d goroutines, got %d", m, relayShards, want, got)
	}
}

// TestAddOutboxRelay_LeaderKeyDistinctPerPhysicalShard asserts the leader-lock
// key strategy: at M=1 the key is byte-identical to the historical bare key
// (no :pshard: suffix), while at M>1 each physical shard gets a distinct
// :pshard:<i> suffix so shard 0's leader and shard 1's leader never collide.
func TestAddOutboxRelay_LeaderKeyDistinctPerPhysicalShard(t *testing.T) {
	// M=1: no suffix — byte-identical to the historical key.
	r1 := outbox.NewRelay(&pg.Pool{}, noopPublisher{}, outbox.RelayConfig{SingleActive: true})
	if got := r1.LeaderLockSQL(); got != outbox.HistoricalLeaderLockSQL {
		t.Fatalf("M=1 leader key must stay historical:\n got %q\nwant %q", got, outbox.HistoricalLeaderLockSQL)
	}

	// M>1: distinct per physical shard.
	r0 := outbox.NewRelay(&pg.Pool{}, noopPublisher{}, outbox.RelayConfig{SingleActive: true}, outbox.WithLeaderKeySuffix(":pshard:0"))
	rb := outbox.NewRelay(&pg.Pool{}, noopPublisher{}, outbox.RelayConfig{SingleActive: true}, outbox.WithLeaderKeySuffix(":pshard:1"))
	if r0.LeaderLockSQL() == rb.LeaderLockSQL() {
		t.Fatalf("physical shards 0 and 1 must have distinct leader keys, both = %q", r0.LeaderLockSQL())
	}
	if r0.LeaderLockSQL() == outbox.HistoricalLeaderLockSQL {
		t.Fatalf("M>1 shard 0 key must differ from the historical bare key")
	}
}
