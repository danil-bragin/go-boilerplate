package money

import (
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"
)

// Money is an immutable monetary amount in a specific asset. All operations
// return a new Money; the receiver is never mutated (safe to share across
// goroutines). Full precision is retained — Add/Sub/Mul never round.
type Money struct {
	amount decimal.Decimal
	asset  string
}

// Parse builds Money from a major-unit decimal string ("12.34") + a registered
// asset code. No float path; full precision retained.
func Parse(s, asset string) (Money, error) {
	if _, ok := Lookup(asset); !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, asset)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Money{}, fmt.Errorf("money: parse %q: %w", s, err)
	}
	return Money{amount: d, asset: asset}, nil
}

// FromMajor is Parse, named for call-site clarity.
func FromMajor(s, asset string) (Money, error) { return Parse(s, asset) }

// FromMinor builds Money from an integer count of the asset's SMALLEST unit
// (cents, wei): value = minor × 10^-exponent (exact).
func FromMinor(minor *big.Int, asset string) (Money, error) {
	if minor == nil {
		return Money{}, errNilAmount
	}
	a, ok := Lookup(asset)
	if !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, asset)
	}
	return Money{amount: decimal.NewFromBigInt(minor, -a.Exponent), asset: asset}, nil
}

// Zero is the zero amount of asset.
func Zero(asset string) (Money, error) { return Parse("0", asset) }

// MustParse is Parse for compile-time-known constants/tests; panics on bad input.
func MustParse(s, asset string) Money {
	m, err := Parse(s, asset)
	if err != nil {
		panic(err)
	}
	return m
}

// Asset returns the asset code.
func (m Money) Asset() string { return m.asset }

// String returns "<amount> <asset>", e.g. "12.34 USD".
func (m Money) String() string { return m.amount.String() + " " + m.asset }
