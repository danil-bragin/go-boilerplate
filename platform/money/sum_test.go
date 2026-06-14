package money

import (
	"errors"
	"testing"
)

func TestSum(t *testing.T) {
	got, err := Sum(
		MustParse("19.99", "USD"),
		MustParse("5.00", "USD"),
		MustParse("0.99", "USD"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(MustParse("25.98", "USD")) {
		t.Fatalf("Sum = %s, want 25.98 USD", got)
	}
}

func TestSum_Single(t *testing.T) {
	got, err := Sum(MustParse("42.00", "EUR"))
	if err != nil || !got.Equal(MustParse("42", "EUR")) {
		t.Fatalf("Sum single = %s err=%v", got, err)
	}
}

func TestSum_Empty(t *testing.T) {
	if _, err := Sum(); !errors.Is(err, &Error{Code: CodeInvalidAmount}) {
		t.Fatalf("Sum() want CodeInvalidAmount, got %v", err)
	}
}

func TestSum_CrossAsset(t *testing.T) {
	_, err := Sum(MustParse("1", "USD"), MustParse("1", "EUR"))
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Sum cross-asset want ErrCurrencyMismatch, got %v", err)
	}
}

func TestSum_SliceSpread(t *testing.T) {
	xs := []Money{MustParse("1.10", "USD"), MustParse("2.20", "USD"), MustParse("3.30", "USD")}
	got, err := Sum(xs...)
	if err != nil || !got.Equal(MustParse("6.60", "USD")) {
		t.Fatalf("Sum(slice...) = %s err=%v", got, err)
	}
}
