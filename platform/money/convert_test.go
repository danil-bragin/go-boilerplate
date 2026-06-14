package money

import (
	"errors"
	"testing"
)

func TestConvert(t *testing.T) {
	got, err := MustParse("100", "USD").Convert(Rate{From: "USD", To: "EUR", Factor: MustDec("0.92")})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if g := got.String(); g != "92 EUR" {
		t.Fatalf("100 USD * 0.92 = 92 EUR: %s", g)
	}
}

func TestConvert_FullPrecision(t *testing.T) {
	got, err := MustParse("100", "USD").Convert(Rate{From: "USD", To: "EUR", Factor: MustDec("0.925")})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if g := got.String(); g != "92.5 EUR" {
		t.Fatalf("full precision 100 * 0.925 = 92.5 EUR: %s", g)
	}
}

func TestConvert_WrongFrom(t *testing.T) {
	_, err := MustParse("100", "USD").Convert(Rate{From: "GBP", To: "EUR", Factor: MustDec("1.1")})
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("wrong From want ErrCurrencyMismatch, got %v", err)
	}
}

func TestConvert_UnknownTo(t *testing.T) {
	_, err := MustParse("100", "USD").Convert(Rate{From: "USD", To: "ZZZ", Factor: MustDec("1.1")})
	if !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("unknown To want ErrUnknownAsset, got %v", err)
	}
}
