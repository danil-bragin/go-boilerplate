package money

import "testing"

func TestAssetRegistry_BuiltinExponents(t *testing.T) {
	for code, want := range map[string]int32{"USD": 2, "EUR": 2, "GBP": 2, "JPY": 0, "KRW": 0, "BHD": 3, "KWD": 3, "BTC": 8, "ETH": 18, "USDT": 6} {
		a, ok := Lookup(code)
		if !ok {
			t.Fatalf("%s not registered", code)
		}
		if a.Exponent != want {
			t.Fatalf("%s: want exp %d got %d", code, want, a.Exponent)
		}
	}
}

func TestAssetRegistry_RegisterCustomAndConflict(t *testing.T) {
	if err := Register(Asset{Code: "MYTOK", Exponent: 9, Kind: Crypto}); err != nil {
		t.Fatal(err)
	}
	a, ok := Lookup("MYTOK")
	if !ok || a.Exponent != 9 {
		t.Fatal("custom not registered")
	}
	if err := Register(Asset{Code: "MYTOK", Exponent: 9, Kind: Crypto}); err != nil {
		t.Fatalf("idempotent re-register ok: %v", err)
	}
	if err := Register(Asset{Code: "MYTOK", Exponent: 2, Kind: Crypto}); err == nil {
		t.Fatal("conflicting re-register must error")
	}
	if err := Register(Asset{Code: "", Exponent: 2, Kind: Fiat}); err == nil {
		t.Fatal("empty code must error")
	}
	if err := Register(Asset{Code: "NEG", Exponent: -1, Kind: Fiat}); err == nil {
		t.Fatal("negative exponent must error")
	}
}
