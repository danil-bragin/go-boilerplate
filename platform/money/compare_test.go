package money

import (
	"errors"
	"testing"
)

func TestCmp(t *testing.T) {
	lo := MustParse("1.00", "USD")
	hi := MustParse("2.00", "USD")
	if c, err := lo.Cmp(hi); err != nil || c != -1 {
		t.Fatalf("lo<hi: c=%d err=%v", c, err)
	}
	if c, err := hi.Cmp(lo); err != nil || c != 1 {
		t.Fatalf("hi>lo: c=%d err=%v", c, err)
	}
	if c, err := lo.Cmp(MustParse("1.00", "USD")); err != nil || c != 0 {
		t.Fatalf("eq: c=%d err=%v", c, err)
	}
}

func TestCmp_CrossAsset(t *testing.T) {
	if _, err := MustParse("1", "USD").Cmp(MustParse("1", "EUR")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("cross-asset Cmp want ErrCurrencyMismatch, got %v", err)
	}
}

func TestComparators(t *testing.T) {
	lo := MustParse("1.00", "USD")
	hi := MustParse("2.00", "USD")
	eq := MustParse("1.00", "USD")

	cases := []struct {
		name string
		got  func() (bool, error)
		want bool
	}{
		{"lo<hi", func() (bool, error) { return lo.LessThan(hi) }, true},
		{"hi<lo", func() (bool, error) { return hi.LessThan(lo) }, false},
		{"lo<=eq", func() (bool, error) { return lo.LessThanOrEqual(eq) }, true},
		{"hi<=lo", func() (bool, error) { return hi.LessThanOrEqual(lo) }, false},
		{"hi>lo", func() (bool, error) { return hi.GreaterThan(lo) }, true},
		{"lo>hi", func() (bool, error) { return lo.GreaterThan(hi) }, false},
		{"lo>=eq", func() (bool, error) { return lo.GreaterThanOrEqual(eq) }, true},
		{"lo>=hi", func() (bool, error) { return lo.GreaterThanOrEqual(hi) }, false},
	}
	for _, c := range cases {
		got, err := c.got()
		if err != nil || got != c.want {
			t.Fatalf("%s: got=%v want=%v err=%v", c.name, got, c.want, err)
		}
	}
}

func TestComparators_CrossAsset(t *testing.T) {
	usd := MustParse("1", "USD")
	eur := MustParse("1", "EUR")
	probes := []func() (bool, error){
		func() (bool, error) { return usd.LessThan(eur) },
		func() (bool, error) { return usd.LessThanOrEqual(eur) },
		func() (bool, error) { return usd.GreaterThan(eur) },
		func() (bool, error) { return usd.GreaterThanOrEqual(eur) },
	}
	for i, p := range probes {
		if _, err := p(); !errors.Is(err, ErrCurrencyMismatch) {
			t.Fatalf("probe %d want ErrCurrencyMismatch, got %v", i, err)
		}
	}
}

func TestMinMax(t *testing.T) {
	lo := MustParse("1.00", "USD")
	hi := MustParse("2.00", "USD")

	if m, err := lo.Min(hi); err != nil || !m.Equal(lo) {
		t.Fatalf("Min lo,hi: %v err=%v", m, err)
	}
	if m, err := hi.Min(lo); err != nil || !m.Equal(lo) {
		t.Fatalf("Min hi,lo: %v err=%v", m, err)
	}
	if m, err := lo.Max(hi); err != nil || !m.Equal(hi) {
		t.Fatalf("Max lo,hi: %v err=%v", m, err)
	}
	if m, err := hi.Max(lo); err != nil || !m.Equal(hi) {
		t.Fatalf("Max hi,lo: %v err=%v", m, err)
	}
	// tie returns the receiver
	tie := MustParse("1.00", "USD")
	if m, err := lo.Min(tie); err != nil || !m.Equal(lo) {
		t.Fatalf("Min tie: %v err=%v", m, err)
	}
	if m, err := lo.Max(tie); err != nil || !m.Equal(lo) {
		t.Fatalf("Max tie: %v err=%v", m, err)
	}
	// cross-asset errors
	eur := MustParse("1", "EUR")
	if _, err := lo.Min(eur); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Min cross-asset want ErrCurrencyMismatch, got %v", err)
	}
	if _, err := lo.Max(eur); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Max cross-asset want ErrCurrencyMismatch, got %v", err)
	}
}

func TestEqual(t *testing.T) {
	// shopspring Equal compares by value not scale: 0.10 == 0.1.
	if !MustParse("0.10", "USD").Equal(MustParse("0.1", "USD")) {
		t.Fatal("0.10 should equal 0.1 (same asset, by value)")
	}
	if MustParse("1.00", "USD").Equal(MustParse("2.00", "USD")) {
		t.Fatal("1.00 should not equal 2.00")
	}
	// cross-asset is never equal, even with equal amounts.
	if MustParse("1.00", "USD").Equal(MustParse("1.00", "EUR")) {
		t.Fatal("cross-asset Equal must be false")
	}
}

func TestPredicates(t *testing.T) {
	zero := MustParse("0", "USD")
	neg := MustParse("-1.50", "USD")
	pos := MustParse("1.50", "USD")

	if !zero.IsZero() || zero.IsNegative() || zero.IsPositive() || zero.Sign() != 0 {
		t.Fatalf("zero predicates wrong: sign=%d", zero.Sign())
	}
	if neg.IsZero() || !neg.IsNegative() || neg.IsPositive() || neg.Sign() != -1 {
		t.Fatalf("neg predicates wrong: sign=%d", neg.Sign())
	}
	if pos.IsZero() || pos.IsNegative() || !pos.IsPositive() || pos.Sign() != 1 {
		t.Fatalf("pos predicates wrong: sign=%d", pos.Sign())
	}
}
