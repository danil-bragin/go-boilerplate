package money

import "fmt"

func (m Money) sameAsset(n Money) error {
	if m.asset != n.asset {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.asset, n.asset)
	}
	return nil
}

// Add returns m+n exactly (never rounds). Same-asset only.
func (m Money) Add(n Money) (Money, error) {
	if err := m.sameAsset(n); err != nil {
		return Money{}, err
	}
	return Money{amount: m.amount.Add(n.amount), asset: m.asset}, nil
}

// Sub returns m-n exactly. Same-asset only.
func (m Money) Sub(n Money) (Money, error) {
	if err := m.sameAsset(n); err != nil {
		return Money{}, err
	}
	return Money{amount: m.amount.Sub(n.amount), asset: m.asset}, nil
}

// Mul scales m by an exact decimal factor (quantity/rate). EXACT: the result's
// scale grows; it is NOT rounded. Round explicitly at the boundary.
func (m Money) Mul(factor Dec) Money {
	return Money{amount: m.amount.Mul(factor.d), asset: m.asset}
}

// DivRound divides m by an exact divisor to scale fractional digits under mode.
// Division is the ONLY arithmetic that rounds (it cannot be exact in general) —
// scale + mode are explicit. To divide money AMONG recipients use Split/Allocate
// (which conserve units), not DivRound.
func (m Money) DivRound(divisor Dec, scale int32, mode RoundingMode) Money {
	q := Dec{m.amount.Div(divisor.d)}
	return Money{amount: mode.apply(q, scale).d, asset: m.asset}
}

// Neg returns -m.
func (m Money) Neg() Money { return Money{amount: m.amount.Neg(), asset: m.asset} }

// Abs returns |m|.
func (m Money) Abs() Money { return Money{amount: m.amount.Abs(), asset: m.asset} }
