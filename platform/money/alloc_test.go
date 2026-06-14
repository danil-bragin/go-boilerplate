package money

import "testing"

func TestSplit_ConservesNoCentLost(t *testing.T) {
	ten := MustParse("10.00", "USD")
	parts, err := ten.Split(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("parts %d", len(parts))
	}
	sum := MustParse("0", "USD")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	// sum must conserve the total (compare by string, value-stable form is "10").
	if sum.String() != "10 USD" {
		t.Fatalf("split must conserve total, got %s", sum.String())
	}
	// Fowler: 10.00/3 -> 3.34, 3.33, 3.33
	if parts[0].String() != "3.34 USD" || parts[1].String() != "3.33 USD" || parts[2].String() != "3.33 USD" {
		t.Fatalf("Fowler distribution wrong: %v", []string{parts[0].String(), parts[1].String(), parts[2].String()})
	}
}

func TestAllocate_WeightedConserves(t *testing.T) {
	pot := MustParse("0.05", "USD") // 5 cents, weights 3:2
	parts, err := pot.Allocate(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "USD")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if sum.String() != "0.05 USD" {
		t.Fatalf("allocate must conserve: %s", sum.String())
	}
	// 5 cents 3:2 -> 3 and 2 cents.
	if parts[0].String() != "0.03 USD" || parts[1].String() != "0.02 USD" {
		t.Fatalf("weighted allocate wrong: %v", []string{parts[0].String(), parts[1].String()})
	}
}

func TestSplit_CryptoConserves18dp(t *testing.T) {
	m := MustParse("0.000000000000000010", "ETH") // 10 wei
	parts, err := m.Split(3)                      // 4,3,3 wei
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "ETH")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if sum.String() != m.String() {
		t.Fatalf("crypto split must conserve: %s vs %s", sum, m)
	}
	// Fowler: 10 wei / 3 -> 4,3,3 wei.
	if parts[0].String() != "0.000000000000000004 ETH" ||
		parts[1].String() != "0.000000000000000003 ETH" ||
		parts[2].String() != "0.000000000000000003 ETH" {
		t.Fatalf("crypto Fowler distribution wrong: %v", []string{parts[0].String(), parts[1].String(), parts[2].String()})
	}
}

func TestSplit_Errors(t *testing.T) {
	if _, err := MustParse("1", "USD").Split(0); err == nil {
		t.Fatal("split(0) must error")
	}
	if _, err := MustParse("1", "USD").Allocate(); err == nil {
		t.Fatal("empty ratios must error")
	}
	if _, err := MustParse("1", "USD").Allocate(0, 0); err == nil {
		t.Fatal("ratios summing to zero must error")
	}
	if _, err := MustParse("1", "USD").Allocate(1, -1); err == nil {
		t.Fatal("negative ratio must error")
	}
}

func TestSplit_NegativeConserves(t *testing.T) {
	// A negative amount (refund/reversal) splits with conservation: parts are
	// negative and sum back to the whole exactly.
	refund := MustParse("-10.00", "USD")
	parts, err := refund.Split(3)
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "USD")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if sum.String() != "-10 USD" {
		t.Fatalf("negative split must conserve, got %s", sum.String())
	}
	// Fowler on the magnitude, sign reapplied: -3.34, -3.33, -3.33.
	if parts[0].String() != "-3.34 USD" || parts[1].String() != "-3.33 USD" || parts[2].String() != "-3.33 USD" {
		t.Fatalf("negative Fowler distribution wrong: %s %s %s", parts[0], parts[1], parts[2])
	}
}

func TestAllocate_NegativeWeightedConserves(t *testing.T) {
	pot := MustParse("-0.05", "USD")
	parts, err := pot.Allocate(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "USD")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if sum.String() != "-0.05 USD" {
		t.Fatalf("negative allocate must conserve, got %s", sum.String())
	}
}

func TestSplit_NegativeCrypto18dpConserves(t *testing.T) {
	m := MustParse("-0.000000000000000010", "ETH") // -10 wei
	parts, err := m.Split(3)
	if err != nil {
		t.Fatal(err)
	}
	sum := MustParse("0", "ETH")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if !sum.Equal(m) {
		t.Fatalf("negative crypto split must conserve: %s vs %s", sum, m)
	}
}
