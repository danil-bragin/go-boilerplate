package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultIdleTTL    = 10 * time.Minute
	defaultMaxEntries = 100_000
)

// entry holds a per-key rate limiter and the last time the key was seen.
type entry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// Memory is a per-key in-memory rate limiter backed by golang.org/x/time/rate.
// It is safe for concurrent use. A janitor goroutine evicts entries that have
// been idle longer than IdleTTL to prevent unbounded growth.
type Memory struct {
	mu         sync.Mutex
	entries    map[string]*entry
	rps        float64
	burst      int
	idleTTL    time.Duration
	maxEntries int
	now        func() time.Time
	stopCh     chan struct{}
}

// MemoryOption configures a Memory limiter.
type MemoryOption func(*Memory)

// WithIdleTTL sets the idle-eviction TTL (default 10m).
// Entries not accessed within this duration are evicted by the janitor.
func WithIdleTTL(d time.Duration) MemoryOption {
	return func(m *Memory) { m.idleTTL = d }
}

// WithMaxEntries sets the maximum number of keys held in memory (default 100_000).
// When the limit is reached, a random-sample eviction removes one entry before
// inserting the new key (see insertLocked for details).
func WithMaxEntries(n int) MemoryOption {
	return func(m *Memory) { m.maxEntries = n }
}

// WithClock overrides the clock used for rate-limiting and eviction bookkeeping
// (default time.Now). Injecting a clock makes Allow fully deterministic in
// tests without real sleeps.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *Memory) { m.now = now }
}

// NewMemory constructs a Memory rate limiter that allows rps tokens per second
// with a burst of burst tokens per key. The janitor is started immediately;
// call Close to stop it.
func NewMemory(rps float64, burst int, opts ...MemoryOption) *Memory {
	m := &Memory{
		entries:    make(map[string]*entry),
		rps:        rps,
		burst:      burst,
		idleTTL:    defaultIdleTTL,
		maxEntries: defaultMaxEntries,
		now:        time.Now,
		stopCh:     make(chan struct{}),
	}
	for _, o := range opts {
		o(m)
	}
	go m.janitor()
	return m
}

// Allow returns (true, nil) if key is allowed to proceed, (false, nil) otherwise.
// It never returns a non-nil error; the error return exists to satisfy Limiter.
func (m *Memory) Allow(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	e := m.getLocked(key)
	now := m.now()
	e.lastSeen = now
	ok := e.lim.AllowN(now, 1)
	m.mu.Unlock()
	return ok, nil
}

// Close stops the background eviction janitor.
func (m *Memory) Close() {
	close(m.stopCh)
}

// getLocked returns (or creates) the entry for key. Must be called with m.mu held.
func (m *Memory) getLocked(key string) *entry {
	if e, ok := m.entries[key]; ok {
		return e
	}
	// Cap enforcement: when at max, evict one entry before inserting.
	if len(m.entries) >= m.maxEntries {
		m.evictOneLocked()
	}
	e := &entry{
		lim:      rate.NewLimiter(rate.Limit(m.rps), m.burst),
		lastSeen: m.now(),
	}
	m.entries[key] = e
	return e
}

// evictOneLocked removes one entry using random-sample eviction.
// Must be called with m.mu held.
//
// Why not linear scan? At maxEntries=100_000 a full scan on every insertion
// is O(n) in the hot path — under an attack that rotates keys this becomes
// a sustained O(n) burden. Random-sample eviction (pick ~8 random entries,
// evict the oldest among them) gives O(1) amortised cost while still making
// reasonable eviction choices.
func (m *Memory) evictOneLocked() {
	const sampleSize = 8
	var oldest string
	var oldestTime time.Time
	sampled := 0
	for k, e := range m.entries {
		if sampled == 0 || e.lastSeen.Before(oldestTime) {
			oldest = k
			oldestTime = e.lastSeen
		}
		sampled++
		if sampled >= sampleSize {
			break
		}
	}
	if oldest != "" {
		delete(m.entries, oldest)
	}
}

// evictIdle removes all entries whose lastSeen is older than idleTTL.
// It is exported for testing (allows direct invocation instead of relying on
// timing). The name uses a lower-case package prefix so it is not part of the
// public surface but is accessible from tests in the same package.
func (m *Memory) evictIdle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictIdleLocked()
}

// evictIdleLocked removes idle entries. Must be called with m.mu held.
func (m *Memory) evictIdleLocked() {
	threshold := m.now().Add(-m.idleTTL)
	for k, e := range m.entries {
		if e.lastSeen.Before(threshold) {
			delete(m.entries, k)
		}
	}
}

// janitor runs the background idle-eviction loop.
func (m *Memory) janitor() {
	ticker := time.NewTicker(m.idleTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.evictIdle()
		case <-m.stopCh:
			return
		}
	}
}
