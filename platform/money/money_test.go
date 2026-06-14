package money

import (
	"math/big"
	"testing"
)

func TestMoney_ConstructAndAccess(t *testing.T) {
	m, err := Parse("12.34", "USD")
	if err != nil {
		t.Fatal(err)
	}
	if m.Asset() != "USD" {
		t.Fatalf("asset %s", m.Asset())
	}
	if m.String() != "12.34 USD" {
		t.Fatalf("string %s", m.String())
	}
}

func TestMoney_UnknownAsset(t *testing.T) {
	if _, err := Parse("1", "ZZZ"); err == nil {
		t.Fatal("unknown asset must error")
	}
}

func TestMoney_FromMinorCrypto(t *testing.T) {
	wei, _ := new(big.Int).SetString("1000000000000000000", 10) // 1e18
	m, err := FromMinor(wei, "ETH")
	if err != nil {
		t.Fatal(err)
	}
	if m.String() != "1 ETH" {
		t.Fatalf("1e18 wei = 1 ETH, got %s", m.String())
	}
}

func TestMoney_NoFloatExact(t *testing.T) {
	// foundational precision guard (Add comes in T4, but Parse must be exact)
	if MustParse("0.1", "USD").String() != "0.1 USD" {
		t.Fatal("0.1 not exact")
	}
}
