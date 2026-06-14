package money

import (
	"errors"
	"math/big"
	"testing"
)

func TestMinor_FiatAndCrypto(t *testing.T) {
	cases := []struct {
		amount, asset, want string
	}{
		{"12.34", "USD", "1234"},              // cents
		{"100", "JPY", "100"},                 // 0dp asset
		{"1.5", "ETH", "1500000000000000000"}, // wei (18dp)
		{"0.00000001", "BTC", "1"},            // 1 satoshi
		{"-2.05", "USD", "-205"},              // negative
	}
	for _, c := range cases {
		got, err := MustParse(c.amount, c.asset).Minor()
		if err != nil {
			t.Fatalf("Minor(%s %s): %v", c.amount, c.asset, err)
		}
		if got.String() != c.want {
			t.Fatalf("Minor(%s %s) = %s, want %s", c.amount, c.asset, got, c.want)
		}
	}
}

func TestMinor_RoundTripWithFromMinor(t *testing.T) {
	wei, _ := new(big.Int).SetString("1280000000000000000", 10)
	m, err := FromMinor(wei, "ETH")
	if err != nil {
		t.Fatal(err)
	}
	back, err := m.Minor()
	if err != nil {
		t.Fatal(err)
	}
	if back.Cmp(wei) != 0 {
		t.Fatalf("round-trip: %s != %s", back, wei)
	}
}

func TestMinor_InexactDustErrors(t *testing.T) {
	dust := MustParse("12.345", "USD") // sub-cent precision
	if !dust.HasSubMinor() {
		t.Fatal("HasSubMinor should be true for sub-cent value")
	}
	if _, err := dust.Minor(); !errors.Is(err, ErrInexactMinor) {
		t.Fatalf("Minor on dust want ErrInexactMinor, got %v", err)
	}
	clean := MustParse("12.34", "USD")
	if clean.HasSubMinor() {
		t.Fatal("HasSubMinor should be false for exact-cent value")
	}
}

func TestMinor_ZeroValueMoney(t *testing.T) {
	var m Money // asset "" — not registered
	if _, err := m.Minor(); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("Minor on zero-value want ErrUnknownAsset, got %v", err)
	}
	if m.HasSubMinor() {
		t.Fatal("HasSubMinor on zero-value should be false")
	}
}
