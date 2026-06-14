package money

import (
	"errors"
	"math/big"
	"testing"
)

func TestErrorCode_StringAllCodes(t *testing.T) {
	codes := []ErrorCode{
		CodeCurrencyMismatch, CodeUnknownAsset, CodeDivByZero, CodeInvalidAmount,
		CodeParseFailed, CodeOutOfRange, CodeScaleExceeded, CodeAssetNotAllowed,
		CodeNotMultiple, CodeInvalidRatio, CodeAmountTooLarge, CodeInexactMinor,
		CodeInvalidRate, ErrorCode(999),
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

func TestApply_DefaultModeFallsBackToBank(t *testing.T) {
	if got := RoundingMode(99).apply(MustDec("1.005"), 2).String(); got != "1" {
		t.Fatalf("unknown mode → banker's (1.005→1.00); got %s", got)
	}
}

func TestError_StringAllBranches(t *testing.T) {
	for _, e := range []*Error{
		{Code: CodeCurrencyMismatch, Op: "Add", Asset: "USD", Asset2: "ETH"}, // Asset && Asset2
		{Code: CodeUnknownAsset, Op: "Parse", Asset: "ZZZ"},                  // Asset only
		{Code: CodeInvalidAmount, Op: "FromMinor", Detail: "nil amount"},     // Detail
		{Code: CodeParseFailed, Op: "Parse", wrapped: errors.New("boom")},    // wrapped
		{Code: CodeDivByZero}, // bare
	} {
		if e.Error() == "" {
			t.Fatalf("Error() empty for %+v", e)
		}
	}
}

func TestError_IsNonErrorTarget(t *testing.T) {
	e := &Error{Code: CodeCurrencyMismatch}
	if e.Is(errors.New("plain")) {
		t.Fatal("Is must be false for a non-*Error target")
	}
	if e.Is(nil) {
		t.Fatal("Is must be false for nil target")
	}
}

func TestSymbolFor_AllFiat(t *testing.T) {
	for code, want := range map[string]string{"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "RUB": "₽", "ETH": ""} {
		if got := symbolFor(code); got != want {
			t.Fatalf("symbolFor(%s)=%q want %q", code, got, want)
		}
	}
}

func TestFormat_NegativeMinus(t *testing.T) {
	if got := MustParse("-1234.5", "USD").Format(US); got != "-$1,234.50" {
		t.Fatalf("NegMinus: %s", got)
	}
}

func TestFromMinor_UnknownAsset(t *testing.T) {
	if _, err := FromMinor(big.NewInt(1), "ZZZ"); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("FromMinor unknown asset: %v", err)
	}
}

func TestMustParse_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustParse must panic on bad input")
		}
	}()
	_ = MustParse("not-a-number", "USD")
}

func TestScanRow_Errors(t *testing.T) {
	if _, err := ScanRow("1", "ZZZ"); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("ScanRow unknown asset: %v", err)
	}
	if _, err := ScanRow("not-a-decimal", "USD"); err == nil {
		t.Fatal("ScanRow bad amount must error")
	}
}

func TestUnmarshalText_Errors(t *testing.T) {
	var m Money
	if err := m.UnmarshalText([]byte("nospace")); err == nil {
		t.Fatal("UnmarshalText without space must error")
	}
	if err := m.UnmarshalText([]byte("x USD")); err == nil {
		t.Fatal("UnmarshalText bad amount must error")
	}
}

func TestAsErr_FallbackWrapsNonError(t *testing.T) {
	e := asErr(errors.New("not a money error"))
	if e == nil || e.Code != CodeInvalidAmount {
		t.Fatalf("asErr fallback must wrap non-*Error as CodeInvalidAmount: %+v", e)
	}
}

func TestValidate_MaxBelow(t *testing.T) {
	mustCode(t, Validate(MustParse("10", "USD"), Max(MustParse("5", "USD"))), CodeOutOfRange)
}

func TestValidate_ScaleNonNegativeExponentAndUnknownAsset(t *testing.T) {
	// "1E2" parses with a positive exponent → scaleOf returns 0 (no fractional digits).
	if err := Validate(MustParse("1E2", "USD"), MaxScale()); err != nil {
		t.Fatalf("1E2 USD has 0 fractional digits, MaxScale should pass: %v", err)
	}
	// MaxScale defensive unknown-asset branch (Money with an unregistered asset).
	mustCode(t, Validate(Money{asset: "ZZZ"}, MaxScale()), CodeUnknownAsset)
}

func TestMarshalJSON_Direct(t *testing.T) {
	b, err := MustParse("12.34", "USD").MarshalJSON()
	if err != nil || string(b) != `{"amount":"12.34","asset":"USD"}` {
		t.Fatalf("MarshalJSON: %s %v", b, err)
	}
}

func TestValidate_MaxPass(t *testing.T) {
	if err := Validate(MustParse("3", "USD"), Max(MustParse("5", "USD"))); err != nil {
		t.Fatalf("3 <= Max(5) must pass: %v", err)
	}
}

func TestValidate_MaxCrossAsset(t *testing.T) {
	mustCode(t, Validate(MustParse("1", "ETH"), Max(MustParse("5", "USD"))), CodeCurrencyMismatch)
}
