package money

import (
	"math/big"
	"strconv"

	"github.com/shopspring/decimal"
)

// Split divides m into n equal-as-possible parts that sum EXACTLY to m, giving
// any indivisible remainder (in the asset's smallest unit) to the earliest parts
// (Fowler's allocate). n must be > 0.
func (m Money) Split(n int) ([]Money, error) {
	if n <= 0 {
		return nil, codeErr(CodeInvalidRatio, "Split").withDetail("needs n > 0, got " + strconv.Itoa(n))
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
// Negative amounts are supported (e.g. splitting a refund/reversal): allocation
// runs on the magnitude and the original sign is reapplied to every part, so
// Σ(parts) == m still holds exactly (a negative whole yields negative parts).
func (m Money) Allocate(ratios ...int) ([]Money, error) {
	if len(ratios) == 0 {
		return nil, codeErr(CodeInvalidRatio, "Allocate").withDetail("needs at least one ratio")
	}
	total := 0
	for _, r := range ratios {
		if r < 0 {
			return nil, codeErr(CodeInvalidRatio, "Allocate").withDetail("negative ratio " + strconv.Itoa(r))
		}
		total += r
	}
	if total == 0 {
		return nil, codeErr(CodeInvalidRatio, "Allocate").withDetail("ratios sum to zero")
	}
	a, _ := Lookup(m.asset) // asset validated at construction
	// integer minor units (truncate any sub-smallest-unit precision toward zero)
	minor := m.amount.Shift(a.Exponent).BigInt()
	sign := minor.Sign()
	abs := new(big.Int).Abs(minor) // allocate the magnitude; reapply sign at the end
	totalBig := big.NewInt(int64(total))
	allocated := new(big.Int)
	shares := make([]*big.Int, len(ratios))
	for i, r := range ratios {
		s := new(big.Int).Mul(abs, big.NewInt(int64(r)))
		s.Div(s, totalBig) // non-negative operands => plain truncation
		shares[i] = s
		allocated.Add(allocated, s)
	}
	// remainder = abs - Σshares; hand out one smallest-unit at a time to the
	// earliest parts (Fowler). remainder is in [0, len(shares)) by construction.
	remainder := new(big.Int).Sub(abs, allocated)
	one := big.NewInt(1)
	for i := 0; remainder.Sign() > 0; i = (i + 1) % len(shares) {
		shares[i].Add(shares[i], one)
		remainder.Sub(remainder, one)
	}
	out := make([]Money, len(ratios))
	for i, s := range shares {
		v := s
		if sign < 0 {
			v = new(big.Int).Neg(s)
		}
		out[i] = Money{amount: decimal.NewFromBigInt(v, -a.Exponent), asset: m.asset}
	}
	return out, nil
}
