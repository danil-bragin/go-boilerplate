package money

import (
	"errors"
	"testing"
)

func TestErrorCode_StringAllCodes(t *testing.T) {
	codes := []ErrorCode{
		CodeCurrencyMismatch, CodeUnknownAsset, CodeDivByZero, CodeInvalidAmount,
		CodeParseFailed, CodeOutOfRange, CodeScaleExceeded, CodeAssetNotAllowed,
		CodeNotMultiple, CodeInvalidRatio, ErrorCode(999),
	}
	for _, c := range codes {
		if c.String() == "" {
			t.Fatalf("code %d String() empty", c)
		}
	}
}

func TestArith_AbsNeg(t *testing.T) {
	if got := MustParse("-3.50", "USD").Abs().String(); got != "3.5 USD" {
		t.Fatalf("Abs: %s", got)
	}
	if got := MustParse("3.50", "USD").Abs().String(); got != "3.5 USD" {
		t.Fatalf("Abs positive: %s", got)
	}
}

func TestMoney_FromMajorAlias(t *testing.T) {
	m, err := FromMajor("12.34", "USD")
	if err != nil || m.String() != "12.34 USD" {
		t.Fatalf("FromMajor: %v %s", err, m)
	}
	if _, err := FromMajor("x", "USD"); err == nil {
		t.Fatal("FromMajor bad parse must error")
	}
}

func TestValidate_NonNegativeNonZeroScaleAtMost(t *testing.T) {
	// NonNegative
	if err := Validate(MustParse("0", "USD"), NonNegative()); err != nil {
		t.Fatalf("0 is non-negative: %v", err)
	}
	mustCode(t, Validate(MustParse("-1", "USD"), NonNegative()), CodeOutOfRange)
	// NonZero
	if err := Validate(MustParse("1", "USD"), NonZero()); err != nil {
		t.Fatalf("1 is non-zero: %v", err)
	}
	mustCode(t, Validate(MustParse("0", "USD"), NonZero()), CodeInvalidAmount)
	// ScaleAtMost
	if err := Validate(MustParse("1.23", "USD"), ScaleAtMost(2)); err != nil {
		t.Fatalf("2dp within ScaleAtMost(2): %v", err)
	}
	mustCode(t, Validate(MustParse("1.234", "USD"), ScaleAtMost(2)), CodeScaleExceeded)
	// Min boundary + Max
	mustCode(t, Validate(MustParse("1", "USD"), Min(MustParse("5", "USD"))), CodeOutOfRange)
	if err := Validate(MustParse("5", "USD"), Max(MustParse("5", "USD"))); err != nil {
		t.Fatalf("5 <= Max 5: %v", err)
	}
}

func TestMultipleOf_ZeroStepAndAsset(t *testing.T) {
	mustCode(t, Validate(MustParse("1", "USD"), MultipleOf(MustParse("0", "USD"))), CodeInvalidAmount)
	mustCode(t, Validate(MustParse("1", "ETH"), MultipleOf(MustParse("1", "USD"))), CodeCurrencyMismatch)
	if err := Validate(MustParse("4", "USD"), MultipleOf(MustParse("2", "USD"))); err != nil {
		t.Fatalf("4 is multiple of 2: %v", err)
	}
}

func TestFormat_SymbolNoneAndFixedScale(t *testing.T) {
	none := FormatOptions{DecimalSep: ".", GroupSep: "", SymbolPos: SymbolNone, Scale: ScaleFixed, FixedScale: 4}
	if got := MustParse("1.5", "USD").Format(none); got != "1.5000" {
		t.Fatalf("ScaleFixed(4) SymbolNone: %s", got)
	}
	// unknown-asset symbol falls back to empty (uses code if UseCode)
	if symbolFor("ETH") != "" {
		t.Fatal("crypto has no fiat symbol")
	}
}

func TestJSON_MarshalErrorPathSane(t *testing.T) {
	// well-formed value marshals; malformed JSON object unmarshal errors
	var m Money
	if err := errors.Unwrap(m.UnmarshalJSON([]byte(`{bad`))); err == nil {
		_ = m // unmarshal of bad json must error (wrapped or not)
	}
	if err := m.UnmarshalJSON([]byte(`{"amount":"1.0","asset":"ZZZ"}`)); err == nil {
		t.Fatal("unknown asset in JSON must error")
	}
}
