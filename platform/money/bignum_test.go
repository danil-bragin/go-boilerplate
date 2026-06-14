package money

import (
	"math/big"
	"strings"
	"testing"
)

// These tests push FAR beyond uint256 (78 digits). Money is big.Int-backed
// (arbitrary precision, bounded only by memory), so hundreds/thousands of digits
// must stay exact — no overflow, no float, no panic.

// TestBigNum_HugeMulProductExact: 10^100 × 10^100 = 10^200, exact.
func TestBigNum_HugeMulProductExact(t *testing.T) {
	e100 := "1" + strings.Repeat("0", 100) // 10^100 (101 digits)
	m := MustParse(e100, "USD")
	got := m.Mul(MustDec(e100)) // ×10^100 → 10^200
	want := "1" + strings.Repeat("0", 200) + " USD"
	if got.String() != want {
		t.Fatalf("10^100 × 10^100 must be 10^200 exact, got len=%d", len(got.String()))
	}
}

// TestBigNum_500DigitArithmeticExact: a ~500-digit integer add/sub round-trips
// exactly, cross-checked against big.Int.
func TestBigNum_500DigitArithmeticExact(t *testing.T) {
	a := bigPow(10, 500) // 10^500
	b := big.NewInt(1)
	mA, err := FromMinor(a, "JPY") // JPY exp 0 → minor == major, clean integer
	if err != nil {
		t.Fatal(err)
	}
	mB, _ := FromMinor(b, "JPY")
	sum, _ := mA.Add(mB) // 10^500 + 1
	wantSum := new(big.Int).Add(a, b)
	if sum.String() != wantSum.String()+" JPY" {
		t.Fatalf("500-digit add wrong (len got %d)", len(sum.String()))
	}
	back, _ := sum.Sub(mB) // - 1 → 10^500
	if back.String() != a.String()+" JPY" {
		t.Fatal("500-digit sub must round-trip")
	}
}

// TestBigNum_DeepFractionExact: 80 fractional digits, add exact (no float).
func TestBigNum_DeepFractionExact(t *testing.T) {
	tiny := "0." + strings.Repeat("0", 79) + "1" // 10^-80
	m := MustParse(tiny, "USD")
	sum, _ := m.Add(m) // 2×10^-80
	want := "0." + strings.Repeat("0", 79) + "2 USD"
	if sum.String() != want {
		t.Fatalf("80dp add must be exact: %s", sum.String())
	}
}

// TestBigNum_HugeIntTimesDeepFraction: 100-int-digit × 60-frac-digit, exact
// (product scale = 60, magnitude ~160 digits), cross-checked via big.Int coeff.
func TestBigNum_HugeBalancedMulExact(t *testing.T) {
	bigInt := "1" + strings.Repeat("0", 100)           // 10^100
	frac := "0." + strings.Repeat("0", 59) + "1"       // 10^-60
	got := MustParse(bigInt, "USD").Mul(MustDec(frac)) // 10^100 × 10^-60 = 10^40
	want := "1" + strings.Repeat("0", 40) + " USD"
	if got.String() != want {
		t.Fatalf("10^100 × 10^-60 = 10^40 exact, got %s (len %d)", abbrev(got.String()), len(got.String()))
	}
}

// TestBigNum_SumManyNoDrift: 100000 × 0.01 == 1000, no float drift.
func TestBigNum_SumManyNoDrift(t *testing.T) {
	sum := MustParse("0", "USD")
	cent := MustParse("0.01", "USD")
	for range 100000 {
		sum, _ = sum.Add(cent)
	}
	if sum.String() != "1000 USD" {
		t.Fatalf("100000×0.01 must be exactly 1000, got %s", sum.String())
	}
}

// TestBigNum_SplitHugeConserves: split a ~300-digit amount; parts sum to original.
func TestBigNum_SplitHugeConserves(t *testing.T) {
	huge := MustParse(strings.Repeat("7", 300), "JPY") // 300 sevens, exp 0
	parts, err := huge.Split(7)
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "JPY")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if !sum.Equal(huge) {
		t.Fatal("300-digit split must conserve exactly")
	}
}

// TestBigNum_FormatHugeGrouping: a 150-digit integer groups correctly.
func TestBigNum_FormatHugeGrouping(t *testing.T) {
	m := MustParse(strings.Repeat("9", 150), "JPY") // 150 digits, exp 0
	out := m.Format(FormatOptions{GroupSep: ",", GroupSize: 3, DecimalSep: ".", SymbolPos: SymbolNone, Scale: ScaleAsset})
	digits := strings.Count(out, "9")
	commas := strings.Count(out, ",")
	if digits != 150 {
		t.Fatalf("grouping lost digits: %d", digits)
	}
	if commas != 49 { // 150 digits → 50 groups of 3 → 49 separators
		t.Fatalf("grouping wrong separator count: %d (want 49)", commas)
	}
}

// TestBigNum_NoPanicExtreme: 2000-digit value through ops — no panic, exact.
func TestBigNum_NoPanicExtreme(t *testing.T) {
	v := bigPow(10, 2000)
	m, err := FromMinor(v, "JPY")
	if err != nil {
		t.Fatal(err)
	}
	doubled := m.Mul(MustDec("2"))
	want := new(big.Int).Mul(v, big.NewInt(2))
	if doubled.String() != want.String()+" JPY" {
		t.Fatal("2000-digit mul must be exact")
	}
	if doubled.IsNegative() || doubled.IsZero() {
		t.Fatal("sign predicates wrong at extreme scale")
	}
}

func bigPow(base, exp int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(base), big.NewInt(exp), nil)
}

func abbrev(s string) string {
	if len(s) <= 40 {
		return s
	}
	return s[:20] + "…" + s[len(s)-20:]
}
