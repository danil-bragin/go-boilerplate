package money

import (
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

// Parse bounds on amounts decoded from (possibly untrusted) text. They are far
// wider than any real monetary value — a 256-bit integer is 78 digits, the
// largest crypto scale is 18 — but tight enough that a tiny hostile string like
// "1e999999999" cannot be expanded into a multi-gigabyte decimal during
// rendering or exponent alignment (a denial-of-service amplification).
const (
	maxParseDigits = 1000 // significant digits in the coefficient
	maxParseExp    = 256  // |power-of-ten exponent| (integer-part or scale)
)

// guardMagnitude rejects a decimal whose coefficient or exponent is so large
// that materializing it would allocate unbounded memory. It guards the text
// parse paths (Parse, and thus JSON/proto, plus ScanRow), where a short input
// can blow up into a huge value. Constructors that already hold a materialized
// *big.Int (FromMinor) are exempt: there is no string amplification there.
func guardMagnitude(d decimal.Decimal, op string) error {
	if d.NumDigits() > maxParseDigits {
		return codeErr(CodeAmountTooLarge, op).withDetail("coefficient exceeds digit bound")
	}
	if e := d.Exponent(); e > maxParseExp || e < -maxParseExp {
		return codeErr(CodeAmountTooLarge, op).withDetail("exponent out of bounds")
	}
	return nil
}

// Parse builds Money from a major-unit decimal string ("12.34") + a registered
// asset code. No float path; full precision retained. The amount is bounded by
// guardMagnitude so untrusted input cannot trigger an unbounded allocation.
func Parse(s, asset string) (Money, error) {
	if _, ok := Lookup(asset); !ok {
		return Money{}, codeErr(CodeUnknownAsset, "Parse").withAsset(asset)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Money{}, codeErr(CodeParseFailed, "Parse").withDetail(s).wrap(err)
	}
	if err := guardMagnitude(d, "Parse"); err != nil {
		return Money{}, err
	}
	return Money{amount: d, asset: asset}, nil
}

// FromMajor is Parse, named for call-site clarity.
func FromMajor(s, asset string) (Money, error) { return Parse(s, asset) }

// FromMinor builds Money from an integer count of the asset's SMALLEST unit
// (cents, wei): value = minor × 10^-exponent (exact).
func FromMinor(minor *big.Int, asset string) (Money, error) {
	if minor == nil {
		return Money{}, codeErr(CodeInvalidAmount, "FromMinor").withDetail("nil amount")
	}
	a, ok := Lookup(asset)
	if !ok {
		return Money{}, codeErr(CodeUnknownAsset, "FromMinor").withAsset(asset)
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

// Minor returns the amount as an exact integer count of the asset's smallest
// unit (cents for USD, wei for ETH): value × 10^exponent. It is the inverse of
// FromMinor and is exact — if the amount carries precision finer than the
// smallest unit (dust), it returns ErrInexactMinor rather than silently
// truncating. Round or Truncate to the asset scale first if truncation is the
// intent. Use this for integer-minor wire formats and PSP/blockchain APIs.
func (m Money) Minor() (*big.Int, error) {
	a, ok := Lookup(m.asset)
	if !ok {
		return nil, codeErr(CodeUnknownAsset, "Minor").withAsset(m.asset)
	}
	shifted := m.amount.Shift(a.Exponent) // value × 10^exponent
	if !shifted.Equal(shifted.Truncate(0)) {
		return nil, codeErr(CodeInexactMinor, "Minor").withAsset(m.asset).
			withDetail("amount has sub-smallest-unit precision")
	}
	return shifted.BigInt(), nil
}

// HasSubMinor reports whether m carries precision finer than the asset's
// smallest unit — i.e. whether Minor would return ErrInexactMinor.
func (m Money) HasSubMinor() bool {
	a, ok := Lookup(m.asset)
	if !ok {
		return false
	}
	shifted := m.amount.Shift(a.Exponent)
	return !shifted.Equal(shifted.Truncate(0))
}

// Asset returns the asset code.
func (m Money) Asset() string { return m.asset }

// String returns "<amount> <asset>", e.g. "12.34 USD".
func (m Money) String() string { return m.amount.String() + " " + m.asset }
