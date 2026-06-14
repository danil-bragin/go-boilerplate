package money

import "testing"

func TestArith_AddSubExact(t *testing.T) {
	a, b := MustParse("0.1", "USD"), MustParse("0.2", "USD")
	sum, err := a.Add(b)
	if err != nil || sum.String() != "0.3 USD" {
		t.Fatalf("0.1+0.2 must be 0.3 USD: %v %s", err, sum)
	}
	d, _ := b.Sub(a)
	if d.String() != "0.1 USD" {
		t.Fatalf("0.2-0.1: %s", d)
	}
}

func TestArith_CrossAssetErrors(t *testing.T) {
	if _, err := MustParse("1", "USD").Add(MustParse("1", "ETH")); err == nil {
		t.Fatal("cross-asset add must error")
	}
	if _, err := MustParse("1", "USD").Sub(MustParse("1", "ETH")); err == nil {
		t.Fatal("cross-asset sub must error")
	}
}

func TestArith_MulExactScaleGrows(t *testing.T) {
	got := MustParse("1.99", "USD").Mul(MustDec("0.0825"))
	if got.String() != "0.164175 USD" {
		t.Fatalf("mul must keep full precision: %s", got)
	}
}

func TestArith_DivRoundExplicit(t *testing.T) {
	got := MustParse("10", "USD").DivRound(MustDec("3"), 2, HalfEven)
	if got.String() != "3.33 USD" {
		t.Fatalf("10/3 @2 HalfEven = 3.33: %s", got)
	}
}

func TestArith_ImmutableReceiver(t *testing.T) {
	a := MustParse("5", "USD")
	_, _ = a.Add(MustParse("3", "USD"))
	_ = a.Mul(MustDec("2"))
	_ = a.Neg()
	if a.String() != "5 USD" {
		t.Fatal("operation mutated the receiver — Money must be immutable")
	}
}

func TestArith_LongChainExact(t *testing.T) {
	// multiply chain stays exact (no intermediate rounding)
	m := MustParse("1", "USD")
	for range 10 {
		m = m.Mul(MustDec("1.1"))
	}
	// 1.1^10 = 2.59374246010000000000 (exact decimal)
	if got := m.Mul(MustDec("1")).String(); got != "2.5937424601 USD" {
		t.Fatalf("long mul chain must be exact, got %s", got)
	}
}
