package money

import (
	"errors"
	"testing"
)

func TestValidate_Rules(t *testing.T) {
	usd := MustParse("10.00", "USD")
	if err := Validate(usd, Positive(), MaxScale(), AllowedAssets("USD", "EUR")); err != nil {
		t.Fatalf("valid money failed: %v", err)
	}

	// Positive fails on a negative amount.
	mustCode(t, Validate(MustParse("-1", "USD"), Positive()), CodeOutOfRange)

	// Between: in range passes, out of range is CodeOutOfRange.
	lo, hi := MustParse("5", "USD"), MustParse("20", "USD")
	if err := Validate(usd, Between(lo, hi)); err != nil {
		t.Fatalf("10 in [5,20]: %v", err)
	}
	mustCode(t, Validate(MustParse("100", "USD"), Between(lo, hi)), CodeOutOfRange)

	// MaxScale: sub-cent USD rejected.
	mustCode(t, Validate(MustParse("1.005", "USD"), MaxScale()), CodeScaleExceeded)

	// AllowedAssets.
	mustCode(t, Validate(MustParse("1", "JPY"), AllowedAssets("USD", "EUR")), CodeAssetNotAllowed)

	// MultipleOf.
	mustCode(t, Validate(MustParse("2.50", "USD"), MultipleOf(MustParse("1.00", "USD"))), CodeNotMultiple)

	// Cross-asset range → currency mismatch.
	mustCode(t, Validate(MustParse("1", "ETH"), Between(lo, hi)), CodeCurrencyMismatch)
}

func TestValidate_Crypto(t *testing.T) {
	// A valid 18-decimal ETH amount passes MaxScale (ETH exponent = 18).
	eth := MustParse("0.123456789012345678", "ETH")
	if err := Validate(eth, MaxScale(), AllowedAssets("ETH")); err != nil {
		t.Fatalf("valid 18dp ETH failed: %v", err)
	}

	// 19 fractional digits exceeds ETH's 18.
	mustCode(t, Validate(MustParse("0.1234567890123456789", "ETH"), MaxScale()), CodeScaleExceeded)

	// AllowedAssets("ETH") rejects a non-ETH asset.
	mustCode(t, Validate(MustParse("1", "USD"), AllowedAssets("ETH")), CodeAssetNotAllowed)
}

func mustCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	var me *Error
	if !errors.As(err, &me) {
		t.Fatalf("want *Error code %v, got %v", want, err)
	}
	if me.Code != want {
		t.Fatalf("want code %v, got %v", want, me.Code)
	}
}
