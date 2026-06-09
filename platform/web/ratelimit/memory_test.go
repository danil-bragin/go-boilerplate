package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock provides a controllable clock for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// TestMemory_PerKeyIsolation verifies that exhausting key A's burst does not
// affect key B.
func TestMemory_PerKeyIsolation(t *testing.T) {
	m := NewMemory(1, 2, WithIdleTTL(time.Hour))
	defer m.Close()
	ctx := context.Background()

	// Exhaust A's burst (burst=2).
	ok, err := m.Allow(ctx, "A")
	require.NoError(t, err)
	assert.True(t, ok, "A first allow")

	ok, err = m.Allow(ctx, "A")
	require.NoError(t, err)
	assert.True(t, ok, "A second allow")

	ok, err = m.Allow(ctx, "A")
	require.NoError(t, err)
	assert.False(t, ok, "A third allow should be denied")

	// B must still be allowed (independent bucket).
	ok, err = m.Allow(ctx, "B")
	require.NoError(t, err)
	assert.True(t, ok, "B must be unaffected by A's exhaustion")
}

// TestMemory_BurstHonoredExactly verifies that exactly burst=3 allows are
// granted before the 4th is denied.
func TestMemory_BurstHonoredExactly(t *testing.T) {
	clock := newFakeClock(time.Now())
	m := NewMemory(
		1, 3,
		WithIdleTTL(time.Hour),
		WithClock(clock.Now),
	)
	defer m.Close()
	ctx := context.Background()

	for i := range 3 {
		ok, err := m.Allow(ctx, "k")
		require.NoError(t, err)
		assert.True(t, ok, "allow %d should succeed", i+1)
	}

	ok, err := m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "4th allow must be denied (burst=3 exhausted)")
}

// TestMemory_RefillViaInjectedClock verifies that advancing the injected clock
// by 1/rps seconds grants exactly one additional token.
func TestMemory_RefillViaInjectedClock(t *testing.T) {
	clock := newFakeClock(time.Now())
	const rps = 2.0
	m := NewMemory(
		rps, 1,
		WithIdleTTL(time.Hour),
		WithClock(clock.Now),
	)
	defer m.Close()
	ctx := context.Background()

	// Consume the single token.
	ok, err := m.Allow(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "first allow (burst=1)")

	// Denied immediately.
	ok, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "second allow must be denied (no tokens)")

	// Advance clock by exactly 1/rps seconds so one token refills.
	clock.Advance(time.Duration(float64(time.Second) / rps))

	ok, err = m.Allow(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok, "allow after clock advance must succeed (token refilled)")
}

// TestMemory_IdleEviction verifies that entries idle past idleTTL are evicted
// when evictIdle is called directly.
func TestMemory_IdleEviction(t *testing.T) {
	clock := newFakeClock(time.Now())
	idleTTL := 5 * time.Minute
	m := NewMemory(
		10, 10,
		WithIdleTTL(idleTTL),
		WithClock(clock.Now),
	)
	defer m.Close()
	ctx := context.Background()

	// Create an entry.
	_, err := m.Allow(ctx, "idle-key")
	require.NoError(t, err)

	m.mu.Lock()
	require.Equal(t, 1, len(m.entries), "entry must be present before eviction")
	m.mu.Unlock()

	// Advance clock past the idle TTL.
	clock.Advance(idleTTL + time.Second)

	// Trigger eviction directly (deterministic, no sleep needed).
	m.evictIdle()

	m.mu.Lock()
	assert.Equal(t, 0, len(m.entries), "idle entry must have been evicted")
	m.mu.Unlock()
}

// TestMemory_MaxEntriesEviction verifies that inserting maxCap+100 keys keeps
// the map at or below maxEntries.
func TestMemory_MaxEntriesEviction(t *testing.T) {
	const maxCap = 50
	m := NewMemory(
		100, 100,
		WithMaxEntries(maxCap),
		WithIdleTTL(time.Hour),
	)
	defer m.Close()
	ctx := context.Background()

	for i := range maxCap + 100 {
		key := "key-" + itoa(i)
		_, err := m.Allow(ctx, key)
		require.NoError(t, err)
	}

	m.mu.Lock()
	n := len(m.entries)
	m.mu.Unlock()

	assert.LessOrEqual(t, n, maxCap, "map must not exceed maxEntries=%d, got %d", maxCap, n)
}

// TestMemory_ConcurrentAllowRaceSafety runs parallel goroutines hitting Allow
// on many keys and relies on -race to catch data races.
func TestMemory_ConcurrentAllowRaceSafety(t *testing.T) {
	t.Helper() // establishes t as used; -race detects any data races inside Allow
	m := NewMemory(1000, 1000, WithIdleTTL(time.Hour))
	defer m.Close()
	ctx := context.Background()

	const goroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				key := "k" + itoa(id%10) + "-" + itoa(i%5)
				_, _ = m.Allow(ctx, key)
			}
		}(g)
	}
	wg.Wait()
}

// itoa is a tiny helper to avoid importing strconv in tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
