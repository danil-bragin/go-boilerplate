package money

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"
)

// Split divides m into n equal-as-possible parts that sum EXACTLY to m, giving
// any indivisible remainder (in the asset's smallest unit) to the earliest parts
// (Fowler's allocate). n must be > 0.
func (m Money) Split(n int) ([]Money, error) {
	if n <= 0 {
		return nil, fmt.Errorf("money: split needs n > 0, got %d", n)
	}
	w := make([]int, n)
	for i := range w {
		w[i] = 1
	}
	return m.Allocate(w...)
}

// Allocate divides m by integer ratios into len(ratios) parts summing EXACTLY to
// m. It operates in the asset's smallest unit so no value is lost or created:
// the conservation guarantee Σ(parts) == m holds at the asset's smallest unit
// (the only sensible divisible granularity). Any sub-smallest-unit precision in
// m is truncated toward zero before allocation.
//
// Allocate requires a non-negative amount; splitting a debt (m < 0) is a
// separate concern and returns an error.
func (m Money) Allocate(ratios ...int) ([]Money, error) {
	if len(ratios) == 0 {
		return nil, errors.New("money: allocate needs at least one ratio")
	}
	total := 0
	for _, r := range ratios {
		if r < 0 {
			return nil, fmt.Errorf("money: allocate negative ratio %d", r)
		}
		total += r
	}
	if total == 0 {
		return nil, errors.New("money: allocate ratios sum to zero")
	}
	a, _ := Lookup(m.asset) // asset validated at construction
	// integer minor units (truncate any sub-smallest-unit precision toward zero)
	minor := m.amount.Shift(a.Exponent).BigInt()
	if minor.Sign() < 0 {
		return nil, fmt.Errorf("money: allocate requires a non-negative amount, got %s", m.String())
	}
	totalBig := big.NewInt(int64(total))
	allocated := new(big.Int)
	out := make([]Money, len(ratios))
	for i, r := range ratios {
		share := new(big.Int).Mul(minor, big.NewInt(int64(r)))
		share.Div(share, totalBig) // floor; non-negative operands => plain truncation
		out[i] = Money{amount: decimal.NewFromBigInt(share, -a.Exponent), asset: m.asset}
		allocated.Add(allocated, share)
	}
	// remainder = minor - Σshares; hand out one smallest-unit at a time to the
	// earliest parts (Fowler). remainder is in [0, len(out)) by construction.
	remainder := new(big.Int).Sub(minor, allocated)
	one := decimal.NewFromBigInt(big.NewInt(1), -a.Exponent) // 10^-exponent
	for i := 0; remainder.Sign() > 0; i = (i + 1) % len(out) {
		out[i] = Money{amount: out[i].amount.Add(one), asset: m.asset}
		remainder.Sub(remainder, big.NewInt(1))
	}
	return out, nil
}
