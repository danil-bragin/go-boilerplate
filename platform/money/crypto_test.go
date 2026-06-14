package money

import (
	"errors"
	"math/big"
	"strings"
	"testing"
)

// bigFromString parses a base-10 integer literal into a *big.Int.
func bigFromString(t *testing.T, s string) *big.Int {
	t.Helper()
	b, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad big int %q", s)
	}
	return b
}

// TestCrypto_Uint256MaxExact verifies that FromMinor accepts a 2^256-1 wei value
// without overflow or precision loss, and that adding 1 wei is exact.
func TestCrypto_Uint256MaxExact(t *testing.T) {
	maxWei := bigFromString(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935")

	m, err := FromMinor(maxWei, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(maxWei, ETH): %v", err)
	}

	oneWei, err := FromMinor(big.NewInt(1), "ETH")
	if err != nil {
		t.Fatalf("FromMinor(1, ETH): %v", err)
	}

	got, err := m.Add(oneWei)
	if err != nil {
		t.Fatalf("Add(oneWei): %v", err)
	}

	// Expected = FromMinor(maxWei + 1, "ETH")
	maxWeiPlus1 := new(big.Int).Add(maxWei, big.NewInt(1))
	want, err := FromMinor(maxWeiPlus1, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(maxWei+1, ETH): %v", err)
	}

	if !got.Equal(want) {
		t.Fatalf("uint256 max + 1 wei mismatch:\n  got  %s\n  want %s", got, want)
	}
}

// TestCrypto_Uint256MulLargeExact verifies that Mul(2) on a uint256-scale Money
// value is exact (big.Int has arbitrary precision — no overflow possible).
func TestCrypto_Uint256MulLargeExact(t *testing.T) {
	maxWei := bigFromString(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935")

	m, err := FromMinor(maxWei, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(maxWei, ETH): %v", err)
	}

	got := m.Mul(MustDec("2"))

	twoMaxWei := new(big.Int).Mul(maxWei, big.NewInt(2))
	want, err := FromMinor(twoMaxWei, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(2*maxWei, ETH): %v", err)
	}

	if !got.Equal(want) {
		t.Fatalf("maxWei * 2 mismatch:\n  got  %s\n  want %s", got, want)
	}
}

// TestCrypto_DustOneWei verifies that a single wei (10^-18 ETH) is represented
// and displayed exactly, and that 1 wei + 1 wei == 2 wei.
func TestCrypto_DustOneWei(t *testing.T) {
	oneWei, err := FromMinor(big.NewInt(1), "ETH")
	if err != nil {
		t.Fatalf("FromMinor(1, ETH): %v", err)
	}

	const wantStr = "0.000000000000000001 ETH"
	if oneWei.String() != wantStr {
		t.Fatalf("1 wei string: got %q, want %q", oneWei.String(), wantStr)
	}

	twoWeiMoney, err := oneWei.Add(oneWei)
	if err != nil {
		t.Fatalf("Add(oneWei, oneWei): %v", err)
	}

	twoWei, err := FromMinor(big.NewInt(2), "ETH")
	if err != nil {
		t.Fatalf("FromMinor(2, ETH): %v", err)
	}

	if !twoWeiMoney.Equal(twoWei) {
		t.Fatalf("1 wei + 1 wei != 2 wei: got %s", twoWeiMoney)
	}
}

// TestCrypto_18dpMulChainExact performs 5 sequential Mul operations on 1 ETH by
// the factor 1.000000000000000001 and pins the exact decimal result. The expected
// value is derived from big.Int: 1000000000000000001^5 over 10^90.
//
// Pinned value (computed: 1000000000000000001^5 =
// 1000000000000000005000000000000000010000000000000000010000000000000000005000000000000000001).
func TestCrypto_18dpMulChainExact(t *testing.T) {
	m := MustParse("1", "ETH")
	factor := MustDec("1.000000000000000001")

	for i := 0; i < 5; i++ {
		m = m.Mul(factor)
	}

	// The expected string is the 90-decimal-place expansion of 1000000000000000001^5 / 10^90.
	// Computed independently via big.Int above; the integer numerator is:
	// 1000000000000000005000000000000000010000000000000000010000000000000000005000000000000000001
	const wantStr = "1.000000000000000005000000000000000010000000000000000010000000000000000005000000000000000001 ETH"
	if m.String() != wantStr {
		t.Fatalf("5x mul chain:\n  got  %s\n  want %s", m.String(), wantStr)
	}
}

// TestCrypto_SplitLargeConserves verifies that Split(7) on a maxWei ETH Money
// value conserves the total: Σ parts == original (Equal).
func TestCrypto_SplitLargeConserves(t *testing.T) {
	maxWei := bigFromString(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935")

	m, err := FromMinor(maxWei, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(maxWei, ETH): %v", err)
	}

	parts, err := m.Split(7)
	if err != nil {
		t.Fatalf("Split(7): %v", err)
	}
	if len(parts) != 7 {
		t.Fatalf("Split(7) returned %d parts", len(parts))
	}

	zero, err := Zero("ETH")
	if err != nil {
		t.Fatalf("Zero(ETH): %v", err)
	}

	sum := zero
	for _, p := range parts {
		sum, err = sum.Add(p)
		if err != nil {
			t.Fatalf("summing parts: %v", err)
		}
	}

	if !sum.Equal(m) {
		t.Fatalf("Split(7) sum mismatch:\n  sum  %s\n  want %s", sum, m)
	}
}

// TestCrypto_SplitDustConserves verifies that Split(3) on 10 wei conserves the
// total at smallest-unit granularity (10 wei → [4wei, 3wei, 3wei]).
func TestCrypto_SplitDustConserves(t *testing.T) {
	tenWeiMoney, err := FromMinor(big.NewInt(10), "ETH")
	if err != nil {
		t.Fatalf("FromMinor(10, ETH): %v", err)
	}

	parts, err := tenWeiMoney.Split(3)
	if err != nil {
		t.Fatalf("Split(3): %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("Split(3) returned %d parts", len(parts))
	}

	zero, err := Zero("ETH")
	if err != nil {
		t.Fatalf("Zero(ETH): %v", err)
	}

	sum := zero
	for _, p := range parts {
		sum, err = sum.Add(p)
		if err != nil {
			t.Fatalf("summing parts: %v", err)
		}
	}

	if !sum.Equal(tenWeiMoney) {
		t.Fatalf("Split(3) of 10 wei: sum %s != 10 wei %s", sum, tenWeiMoney)
	}

	// Fowler: remainder goes to earliest parts; 10 = 3*3 + 1 → [4, 3, 3]
	fourWei, _ := FromMinor(big.NewInt(4), "ETH")
	threeWei, _ := FromMinor(big.NewInt(3), "ETH")
	expected := []Money{fourWei, threeWei, threeWei}
	for i, p := range parts {
		if !p.Equal(expected[i]) {
			t.Errorf("Split(3)[%d]: got %s, want %s", i, p, expected[i])
		}
	}
}

// TestCrypto_FormatCryptoLarge verifies Format(CryptoFmt) at large scale:
//   - maxWei ETH produces a string ending " ETH" whose digit content (stripping
//     " ETH", ".", and leading zeros) reconstructs maxWei exactly.
//   - A known ETH amount with 18 dp renders to its canonical form.
//   - A grouped fiat large value renders with thousands separators.
func TestCrypto_FormatCryptoLarge(t *testing.T) {
	maxWei := bigFromString(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935")

	m, err := FromMinor(maxWei, "ETH")
	if err != nil {
		t.Fatalf("FromMinor(maxWei, ETH): %v", err)
	}

	formatted := m.Format(CryptoFmt)

	if !strings.HasSuffix(formatted, " ETH") {
		t.Fatalf("CryptoFmt output missing \" ETH\" suffix: %q", formatted)
	}

	// Strip " ETH", remove ".", remove leading zeros, reconstruct the integer.
	body := strings.TrimSuffix(formatted, " ETH")
	digits := strings.ReplaceAll(body, ".", "")
	digits = strings.TrimLeft(digits, "0")

	reconstructed, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("could not parse digit content %q", digits)
	}
	if reconstructed.Cmp(maxWei) != 0 {
		t.Fatalf("digit content mismatch:\n  got  %s\n  want %s", reconstructed, maxWei)
	}

	// Known 18dp ETH amount round-trips through CryptoFmt exactly.
	eth18dp := MustParse("1234567.890123456789", "ETH")
	const wantETH = "1234567.890123456789 ETH"
	if got := eth18dp.Format(CryptoFmt); got != wantETH {
		t.Fatalf("18dp ETH Format(CryptoFmt): got %q, want %q", got, wantETH)
	}

	// Large USD with thousands grouping.
	usdLarge := MustParse("12345678.90", "USD")
	const wantUSD = "$12,345,678.90"
	if got := usdLarge.Format(US); got != wantUSD {
		t.Fatalf("large USD Format(US): got %q, want %q", got, wantUSD)
	}
}

// TestCrypto_ValidateCrypto verifies MaxScale, AllowedAssets, and Between
// validators with ETH values.
func TestCrypto_ValidateCrypto(t *testing.T) {
	// 18dp ETH passes MaxScale (scale == exponent == 18).
	eth18dp := MustParse("1.000000000000000001", "ETH")
	if err := Validate(eth18dp, MaxScale()); err != nil {
		t.Fatalf("MaxScale() on 18dp ETH: unexpected error: %v", err)
	}

	// AllowedAssets("ETH","BTC") passes for ETH.
	if err := Validate(eth18dp, AllowedAssets("ETH", "BTC")); err != nil {
		t.Fatalf("AllowedAssets(ETH,BTC) on ETH: unexpected error: %v", err)
	}

	// AllowedAssets("BTC") fails for ETH.
	if err := Validate(eth18dp, AllowedAssets("BTC")); err == nil {
		t.Fatal("AllowedAssets(BTC) on ETH: expected error, got nil")
	} else {
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeAssetNotAllowed {
			t.Fatalf("AllowedAssets(BTC) on ETH: want CodeAssetNotAllowed, got %v", err)
		}
	}

	// Between with ETH bounds: 0.5 ETH <= 1 ETH <= 2 ETH → passes.
	lo := MustParse("0.5", "ETH")
	hi := MustParse("2", "ETH")
	oneETH := MustParse("1", "ETH")
	if err := Validate(oneETH, Between(lo, hi)); err != nil {
		t.Fatalf("Between(0.5,2) on 1 ETH: unexpected error: %v", err)
	}

	// 3 ETH > hi → fails with CodeOutOfRange.
	threeETH := MustParse("3", "ETH")
	if err := Validate(threeETH, Between(lo, hi)); err == nil {
		t.Fatal("Between(0.5,2) on 3 ETH: expected error, got nil")
	} else {
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeOutOfRange {
			t.Fatalf("Between(0.5,2) on 3 ETH: want CodeOutOfRange, got %v", err)
		}
	}

	// 0.1 ETH < lo → fails with CodeOutOfRange.
	tinyETH := MustParse("0.1", "ETH")
	if err := Validate(tinyETH, Between(lo, hi)); err == nil {
		t.Fatal("Between(0.5,2) on 0.1 ETH: expected error, got nil")
	} else {
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeOutOfRange {
			t.Fatalf("Between(0.5,2) on 0.1 ETH: want CodeOutOfRange, got %v", err)
		}
	}
}

// TestCrypto_BTC8dp verifies BTC satoshi-level precision: 1 satoshi == "0.00000001 BTC",
// and 100,000,000 satoshis (1 BTC) round-trips correctly.
func TestCrypto_BTC8dp(t *testing.T) {
	oneSat, err := FromMinor(big.NewInt(1), "BTC")
	if err != nil {
		t.Fatalf("FromMinor(1, BTC): %v", err)
	}

	const wantSat = "0.00000001 BTC"
	if oneSat.String() != wantSat {
		t.Fatalf("1 satoshi string: got %q, want %q", oneSat.String(), wantSat)
	}

	// 100,000,000 satoshis == 1 BTC.
	hundredMSat := bigFromString(t, "100000000")
	oneBTC, err := FromMinor(hundredMSat, "BTC")
	if err != nil {
		t.Fatalf("FromMinor(1e8, BTC): %v", err)
	}

	wantBTC := MustParse("1", "BTC")
	if !oneBTC.Equal(wantBTC) {
		t.Fatalf("100M sat != 1 BTC: got %s", oneBTC)
	}

	// Round-trip: parse "1 BTC", convert back to minor units, compare.
	// MustParse("1","BTC").amount in minor units = 1 * 10^8 = 100000000
	// We verify by checking Equal with the FromMinor value.
	parsed := MustParse("1", "BTC")
	if !parsed.Equal(oneBTC) {
		t.Fatalf("Parse(\"1\",BTC) != FromMinor(1e8,BTC): %s vs %s", parsed, oneBTC)
	}
}
