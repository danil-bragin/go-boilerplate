package money

import (
	"errors"
	"testing"
)

func TestError_IsMatchesByCode(t *testing.T) {
	err := &Error{Code: CodeCurrencyMismatch, Op: "Add", Asset: "USD", Asset2: "ETH"}
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatal("errors.Is must match a sentinel by Code")
	}
	if errors.Is(err, ErrUnknownAsset) {
		t.Fatal("must not match a different code")
	}
}

func TestError_AsExtractsContext(t *testing.T) {
	_, err := MustParse("1", "USD").Add(MustParse("1", "ETH"))
	var me *Error
	if !errors.As(err, &me) {
		t.Fatal("errors.As must extract *money.Error")
	}
	if me.Code != CodeCurrencyMismatch || me.Asset != "USD" || me.Asset2 != "ETH" {
		t.Fatalf("context wrong: %+v", me)
	}
}

func TestError_UnwrapWrapped(t *testing.T) {
	_, err := Parse("not-a-number", "USD")
	var me *Error
	if !errors.As(err, &me) || me.Code != CodeParseFailed {
		t.Fatalf("parse error must be CodeParseFailed: %v", err)
	}
	if me.Unwrap() == nil {
		t.Fatal("parse error must wrap the underlying decimal error")
	}
}

func TestError_String(t *testing.T) {
	e := &Error{Code: CodeCurrencyMismatch, Op: "Add", Asset: "USD", Asset2: "ETH"}
	if e.Error() == "" {
		t.Fatal("Error() must be non-empty and human-readable")
	}
}
