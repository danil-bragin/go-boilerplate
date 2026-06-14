package money

import "fmt"

// Rate is an exact FX factor from one asset to another (supplied by the caller;
// money does no IO).
type Rate struct {
	From, To string
	Factor   Dec
}

// RateProvider supplies rates (callers implement; money never calls it — defined
// here so services share one interface).
type RateProvider interface {
	// Rate returns the conversion rate from one asset to another.
	Rate(from, to string) (Rate, error)
}

// Convert returns m in Rate.To at full precision (= amount × Factor). Round at
// the boundary separately. m.asset must equal Rate.From; Rate.To must be
// registered.
func (m Money) Convert(r Rate) (Money, error) {
	if m.asset != r.From {
		return Money{}, fmt.Errorf("%w: have %s, rate from %s", ErrCurrencyMismatch, m.asset, r.From)
	}
	if _, ok := Lookup(r.To); !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, r.To)
	}
	return Money{amount: m.amount.Mul(r.Factor.d), asset: r.To}, nil
}
