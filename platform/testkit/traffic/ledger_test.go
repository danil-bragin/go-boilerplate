package traffic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastProbes builds Probes with a tight poll interval for tests.
func fastProbes(status func(ctx context.Context, id string) (string, error)) Probes {
	return Probes{OrderStatus: status, PollInterval: 10 * time.Millisecond}
}

func TestLedger_TerminalSatisfiedAfterPolling(t *testing.T) {
	l := NewLedger()
	l.ExpectTerminal("o1", []string{"paid"}, time.Second)

	var calls atomic.Int64
	probes := fastProbes(func(_ context.Context, id string) (string, error) {
		require.Equal(t, "o1", id)
		if calls.Add(1) < 3 {
			return "pending", nil
		}
		return "paid", nil
	})

	violations := l.Verify(context.Background(), probes)
	assert.Empty(t, violations)
	assert.GreaterOrEqual(t, calls.Load(), int64(3), "must poll until the allowed status appears")
}

func TestLedger_TerminalDeadlineViolation(t *testing.T) {
	l := NewLedger()
	l.setSeed(99)
	l.forScenario("happy").ExpectTerminal("o1", []string{"paid"}, 50*time.Millisecond)

	probes := fastProbes(func(context.Context, string) (string, error) { return "created", nil })
	violations := l.Verify(context.Background(), probes)
	require.Len(t, violations, 1)
	v := violations[0]
	assert.Equal(t, "terminal", v.Kind)
	assert.Equal(t, "o1", v.ID)
	assert.Equal(t, "happy", v.Scenario)
	assert.EqualValues(t, 99, v.Seed)
	assert.Contains(t, v.Expected, "paid")
	assert.Contains(t, v.Observed, "created")
	assert.NotEmpty(t, v.String())
}

func TestLedger_TerminalProbeErrorsKeepPolling(t *testing.T) {
	l := NewLedger()
	l.ExpectTerminal("o1", []string{"paid"}, time.Second)

	var calls atomic.Int64
	probes := fastProbes(func(context.Context, string) (string, error) {
		if calls.Add(1) < 3 {
			return "", errors.New("transient")
		}
		return "paid", nil
	})
	assert.Empty(t, l.Verify(context.Background(), probes))
}

func TestLedger_WinnerGroupInvariants(t *testing.T) {
	t.Run("same id repeated is fine", func(t *testing.T) {
		l := NewLedger()
		l.ExpectExactlyOneWinner("key1", "id-A")
		l.ExpectExactlyOneWinner("key1", "id-A")
		assert.Empty(t, l.Verify(context.Background(), Probes{}))
	})

	t.Run("two different ids for one group is the hard violation", func(t *testing.T) {
		l := NewLedger()
		l.setSeed(7)
		h := l.forScenario("mismatch")
		h.ExpectExactlyOneWinner("key1", "id-A")
		h.ExpectExactlyOneWinner("key1", "id-B")
		violations := l.Verify(context.Background(), Probes{})
		require.Len(t, violations, 1)
		assert.Equal(t, "winner", violations[0].Kind)
		assert.Equal(t, "key1", violations[0].ID)
		assert.Equal(t, "mismatch", violations[0].Scenario)
		assert.EqualValues(t, 7, violations[0].Seed)
		assert.Contains(t, violations[0].Observed, "id-A")
		assert.Contains(t, violations[0].Observed, "id-B")
	})

	t.Run("group with only losers has no winner", func(t *testing.T) {
		l := NewLedger()
		l.ObserveLoser("key1")
		violations := l.Verify(context.Background(), Probes{})
		require.Len(t, violations, 1)
		assert.Equal(t, "winner", violations[0].Kind)
	})
}

func TestLedger_RejectedExpectations(t *testing.T) {
	t.Run("matching observation passes", func(t *testing.T) {
		l := NewLedger()
		l.ExpectRejected("op1", "VALIDATION_FAILED")
		l.ObserveRejection("op1", "VALIDATION_FAILED")
		assert.Empty(t, l.Verify(context.Background(), Probes{}))
	})

	t.Run("wrong code is a violation", func(t *testing.T) {
		l := NewLedger()
		l.ExpectRejected("op1", "VALIDATION_FAILED")
		l.ObserveRejection("op1", "HTTP_500")
		violations := l.Verify(context.Background(), Probes{})
		require.Len(t, violations, 1)
		assert.Equal(t, "rejected", violations[0].Kind)
		assert.Equal(t, "VALIDATION_FAILED", violations[0].Expected)
		assert.Equal(t, "HTTP_500", violations[0].Observed)
	})

	t.Run("missing observation is a violation", func(t *testing.T) {
		l := NewLedger()
		l.ExpectRejected("op1", "VALIDATION_FAILED")
		violations := l.Verify(context.Background(), Probes{})
		require.Len(t, violations, 1)
		assert.Equal(t, "rejected", violations[0].Kind)
	})
}

func TestLedger_CountOrdersProbe(t *testing.T) {
	l := NewLedger()
	l.ExpectTerminal("o1", []string{"paid"}, time.Second)
	l.ExpectExactlyOneWinner("k1", "o2")

	var gotIDs []string
	probes := fastProbes(func(context.Context, string) (string, error) { return "paid", nil })
	probes.CountOrders = func(_ context.Context, ids []string) (int, error) {
		gotIDs = ids
		return 1, nil // one row missing → violation
	}

	violations := l.Verify(context.Background(), probes)
	require.Len(t, violations, 1)
	assert.Equal(t, "orders-count", violations[0].Kind)
	assert.ElementsMatch(t, []string{"o1", "o2"}, gotIDs, "unique accepted ids from terminals + winner groups")
	assert.Equal(t, "2", violations[0].Expected)
	assert.Equal(t, "1", violations[0].Observed)
}

func TestLedger_TerminalWithoutProbeIsViolation(t *testing.T) {
	l := NewLedger()
	l.ExpectTerminal("o1", []string{"paid"}, 10*time.Millisecond)
	violations := l.Verify(context.Background(), Probes{})
	require.Len(t, violations, 1)
	assert.Equal(t, "terminal", violations[0].Kind)
}

func TestReservoir_BoundedAndQuantileSanity(t *testing.T) {
	r := newResult(1)
	for i := 1; i <= 1000; i++ {
		r.record("s", time.Duration(i)*time.Millisecond, nil)
	}
	p50, ok := r.Quantile("s", 0.5)
	require.True(t, ok)
	assert.InDelta(t, 500, p50.Milliseconds(), 10)
	p99, ok := r.Quantile("s", 0.99)
	require.True(t, ok)
	assert.InDelta(t, 990, p99.Milliseconds(), 10)

	_, ok = r.Quantile("missing", 0.5)
	assert.False(t, ok)

	// Reservoir stays bounded under maxReservoir overflow.
	big := newResult(1)
	for i := 0; i < maxReservoir+50_000; i++ {
		big.record("s", time.Millisecond, nil)
	}
	assert.LessOrEqual(t, len(big.scenarios["s"].samples), maxReservoir)
	assert.Equal(t, maxReservoir+50_000, big.scenarios["s"].Started)
}
