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
