package money

import "testing"

func TestRound_BankersDefault(t *testing.T) {
	if g := MustParse("1.005", "USD").Round(HalfEven).String(); g != "1 USD" && g != "1.00 USD" {
		t.Fatalf("1.005 banker's→1.00: %s", g)
	}
	if g := MustParse("1.015", "USD").Round(HalfEven).String(); g != "1.02 USD" {
		t.Fatalf("1.015→1.02: %s", g)
	}
	if g := MustParse("1.005", "USD").Round(HalfUp).String(); g != "1.01 USD" {
		t.Fatalf("1.005 halfup→1.01: %s", g)
	}
}

func TestRound_JPYZeroExp(t *testing.T) {
	if g := MustParse("123.7", "JPY").Round(HalfEven).String(); g != "124 JPY" {
		t.Fatalf("JPY exp0: %s", g)
	}
}

func TestWholeFraction(t *testing.T) {
	m := MustParse("3.459", "USD")
	if w := m.Whole().String(); w != "3 USD" {
		t.Fatalf("whole: %s", w)
	}
	if f := m.Fraction().String(); f != "0.459 USD" {
		t.Fatalf("fraction: %s", f)
	}
}

func TestPercent(t *testing.T) {
	if g := MustParse("200", "USD").Percent(MustDec("8.5")).String(); g != "17 USD" {
		t.Fatalf("8.5%% of 200=17: %s", g)
	}
	// exact, no rounding: 50 * 33.333% = 16.6665
	if g := MustParse("50", "USD").Percent(MustDec("33.333")).String(); g != "16.6665 USD" {
		t.Fatalf("percent exact: %s", g)
	}
	// Precision guard (regression): a tiny percentage must NOT be rounded to zero.
	// 1 ETH × 0.0000000000000001% = 0.000000000000000001 (1 wei) — exact, not 0.
	if g := MustParse("1", "ETH").Percent(MustDec("0.0000000000000001")).String(); g != "0.000000000000000001 ETH" {
		t.Fatalf("Percent must stay exact for tiny values (no Div rounding), got %s", g)
	}
	// A high-precision percentage keeps full precision (not truncated at 16dp).
	if g := MustParse("1", "USD").Percent(MustDec("33.333333333333333333")).String(); g != "0.33333333333333333333 USD" {
		t.Fatalf("Percent must keep full precision, got %s", g)
	}
}
