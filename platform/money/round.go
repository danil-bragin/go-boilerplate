package money

import "github.com/shopspring/decimal"

var hundred = decimal.NewFromInt(100)

// Round rounds to the asset's natural exponent under mode (banker's = HalfEven
// is the safe default for money).
func (m Money) Round(mode RoundingMode) Money {
	a, _ := Lookup(m.asset)
	return m.RoundTo(a.Exponent, mode)
}

// RoundTo rounds to scale fractional digits under mode.
func (m Money) RoundTo(scale int32, mode RoundingMode) Money {
	return Money{amount: mode.apply(Dec{m.amount}, scale).d, asset: m.asset}
}

// Truncate cuts to scale fractional digits toward zero (no rounding up).
func (m Money) Truncate(scale int32) Money {
	return Money{amount: m.amount.Truncate(scale), asset: m.asset}
}

// Whole returns m truncated to whole major units (drops the fractional part).
func (m Money) Whole() Money { return m.Truncate(0) }

// Fraction returns the sub-unit remainder: m - Whole(m).
func (m Money) Fraction() Money {
	f, _ := m.Sub(m.Whole())
	return f
}

// Percent returns p percent of m (= m × p/100), EXACT (no rounding). Round at
// the boundary separately if a minor-unit result is required.
func (m Money) Percent(p Dec) Money {
	return m.Mul(Dec{p.d.Div(hundred)})
}
