package money

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestNullMoney_GetOr(t *testing.T) {
	present := ValidMoney(MustParse("12.34", "USD"))
	if m, ok := present.Get(); !ok || !m.Equal(MustParse("12.34", "USD")) {
		t.Fatalf("present Get = %s,%v", m, ok)
	}
	var absent NullMoney
	if m, ok := absent.Get(); ok {
		t.Fatalf("absent Get ok = true (%s)", m)
	}
	def := MustParse("0", "USD")
	if got := present.Or(def); !got.Equal(MustParse("12.34", "USD")) {
		t.Fatalf("present Or = %s", got)
	}
	if got := absent.Or(def); !got.Equal(def) {
		t.Fatalf("absent Or = %s, want default", got)
	}
}

func TestNullMoney_JSON(t *testing.T) {
	// present -> money object, round-trips.
	b, err := json.Marshal(ValidMoney(MustParse("1.50", "ETH")))
	if err != nil {
		t.Fatal(err)
	}
	var back NullMoney
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Valid || !back.Money.Equal(MustParse("1.50", "ETH")) {
		t.Fatalf("present round-trip = %+v", back)
	}
	// absent -> null.
	b, err = json.Marshal(NullMoney{})
	if err != nil || string(b) != "null" {
		t.Fatalf("absent Marshal = %q err=%v", b, err)
	}
	var fromNull NullMoney
	if err := json.Unmarshal([]byte("null"), &fromNull); err != nil || fromNull.Valid {
		t.Fatalf("null Unmarshal = %+v err=%v", fromNull, err)
	}
	// invalid inner amount propagates the Money error.
	var bad NullMoney
	if err := bad.UnmarshalJSON([]byte(`{"amount":"1e1000000000","asset":"USD"}`)); !errors.Is(err, ErrAmountTooLarge) {
		t.Fatalf("bad amount want ErrAmountTooLarge, got %v", err)
	}
}

func TestNullMoney_DBValues(t *testing.T) {
	present := ValidMoney(MustParse("9.99", "USD"))
	v, err := present.AmountValue().Value()
	if err != nil || v == nil {
		t.Fatalf("present AmountValue = %v err=%v", v, err)
	}
	if present.AssetValue() != "USD" {
		t.Fatalf("present AssetValue = %v", present.AssetValue())
	}
	var absent NullMoney
	v, err = absent.AmountValue().Value()
	if err != nil || v != nil {
		t.Fatalf("absent AmountValue = %v err=%v, want nil", v, err)
	}
	if absent.AssetValue() != nil {
		t.Fatalf("absent AssetValue = %v, want nil", absent.AssetValue())
	}
}

func TestScanNullRow(t *testing.T) {
	// both null -> absent
	n, err := ScanNullRow(nil, nil)
	if err != nil || n.Valid {
		t.Fatalf("both-null = %+v err=%v", n, err)
	}
	// both present -> valid
	n, err = ScanNullRow("12.34", "USD")
	if err != nil || !n.Valid || !n.Money.Equal(MustParse("12.34", "USD")) {
		t.Fatalf("both-present = %+v err=%v", n, err)
	}
	// half-null (amount null, asset set) -> error
	if _, err := ScanNullRow(nil, "USD"); !errors.Is(err, &Error{Code: CodeInvalidAmount}) {
		t.Fatalf("half-null amount want CodeInvalidAmount, got %v", err)
	}
	// half-null (asset null, amount set) -> error
	if _, err := ScanNullRow("12.34", nil); !errors.Is(err, &Error{Code: CodeInvalidAmount}) {
		t.Fatalf("half-null asset want CodeInvalidAmount, got %v", err)
	}
	// asset not text -> parse error
	if _, err := ScanNullRow("12.34", 123); !errors.Is(err, &Error{Code: CodeParseFailed}) {
		t.Fatalf("non-text asset want CodeParseFailed, got %v", err)
	}
	// unknown asset -> ScanRow error propagates
	if _, err := ScanNullRow("12.34", "XYZ"); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("unknown asset want ErrUnknownAsset, got %v", err)
	}
}
