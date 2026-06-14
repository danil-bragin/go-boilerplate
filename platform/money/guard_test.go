package money

import (
	"errors"
	"strings"
	"testing"
)

// TestGuard_RejectsHostileExponent: a tiny string with a huge power-of-ten must
// be rejected before it can be expanded into a multi-gigabyte decimal.
func TestGuard_RejectsHostileExponent(t *testing.T) {
	for _, s := range []string{"1e1000000000", "1e257", "1E300", "0.1e-1000"} {
		if _, err := Parse(s, "USD"); !errors.Is(err, ErrAmountTooLarge) {
			t.Fatalf("Parse(%q) want ErrAmountTooLarge, got %v", s, err)
		}
	}
}

// TestGuard_RejectsTooManyDigits: an over-long coefficient is rejected.
func TestGuard_RejectsTooManyDigits(t *testing.T) {
	huge := strings.Repeat("9", maxParseDigits+1) // 1001 digits, exp 0
	if _, err := Parse(huge, "JPY"); !errors.Is(err, ErrAmountTooLarge) {
		t.Fatalf("over-long coefficient want ErrAmountTooLarge, got %v", err)
	}
	// huge fractional scale (exponent below -maxParseExp) is rejected too.
	deepFrac := "0." + strings.Repeat("0", maxParseExp) + "1" // scale maxParseExp+1
	if _, err := Parse(deepFrac, "USD"); !errors.Is(err, ErrAmountTooLarge) {
		t.Fatalf("over-deep fraction want ErrAmountTooLarge, got %v", err)
	}
}

// TestGuard_AllowsBoundaryAndRealisticValues: the bounds are wide enough for any
// real money (256-bit ints are 78 digits, crypto scale is 18).
func TestGuard_AllowsBoundaryAndRealisticValues(t *testing.T) {
	atDigitLimit := strings.Repeat("7", maxParseDigits) // exactly 1000 digits, exp 0
	if _, err := Parse(atDigitLimit, "JPY"); err != nil {
		t.Fatalf("value at digit limit must parse: %v", err)
	}
	atScaleLimit := "0." + strings.Repeat("0", maxParseExp-1) + "1" // scale == maxParseExp
	if _, err := Parse(atScaleLimit, "USD"); err != nil {
		t.Fatalf("value at scale limit must parse: %v", err)
	}
	for _, s := range []string{"0", "12.34", "0.000000000000000001", "21000000.00000000"} {
		if _, err := Parse(s, "BTC"); err != nil {
			t.Fatalf("realistic value %q must parse: %v", s, err)
		}
	}
}

// TestGuard_AppliesToScanRow: the DB scan path is guarded too.
func TestGuard_AppliesToScanRow(t *testing.T) {
	if _, err := ScanRow("1e1000000000", "USD"); !errors.Is(err, ErrAmountTooLarge) {
		t.Fatalf("ScanRow hostile exponent want ErrAmountTooLarge, got %v", err)
	}
}

// TestGuard_AppliesToJSON: UnmarshalJSON routes through Parse, so it inherits
// the guard — untrusted wire input cannot OOM the service.
func TestGuard_AppliesToJSON(t *testing.T) {
	var m Money
	err := m.UnmarshalJSON([]byte(`{"amount":"1e1000000000","asset":"USD"}`))
	if !errors.Is(err, ErrAmountTooLarge) {
		t.Fatalf("UnmarshalJSON hostile amount want ErrAmountTooLarge, got %v", err)
	}
}
