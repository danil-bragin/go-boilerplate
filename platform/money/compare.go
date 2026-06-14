package money

// Cmp compares m to n (same-asset): -1, 0, +1. Errors on cross-asset.
func (m Money) Cmp(n Money) (int, error) {
	if err := m.sameAsset(n); err != nil {
		return 0, err
	}
	return m.amount.Cmp(n.amount), nil
}

// LessThan reports whether m < n (same-asset). Errors on cross-asset.
func (m Money) LessThan(n Money) (bool, error) {
	c, err := m.Cmp(n)
	return c < 0, err
}

// LessThanOrEqual reports whether m <= n (same-asset). Errors on cross-asset.
func (m Money) LessThanOrEqual(n Money) (bool, error) {
	c, err := m.Cmp(n)
	return c <= 0, err
}

// GreaterThan reports whether m > n (same-asset). Errors on cross-asset.
func (m Money) GreaterThan(n Money) (bool, error) {
	c, err := m.Cmp(n)
	return c > 0, err
}

// GreaterThanOrEqual reports whether m >= n (same-asset). Errors on cross-asset.
func (m Money) GreaterThanOrEqual(n Money) (bool, error) {
	c, err := m.Cmp(n)
	return c >= 0, err
}

// Min returns the smaller of m and n (same-asset). Errors on cross-asset. On a
// tie it returns m.
func (m Money) Min(n Money) (Money, error) {
	c, err := m.Cmp(n)
	if err != nil {
		return Money{}, err
	}
	if c <= 0 {
		return m, nil
	}
	return n, nil
}

// Max returns the larger of m and n (same-asset). Errors on cross-asset. On a
// tie it returns m.
func (m Money) Max(n Money) (Money, error) {
	c, err := m.Cmp(n)
	if err != nil {
		return Money{}, err
	}
	if c >= 0 {
		return m, nil
	}
	return n, nil
}

// Equal reports whether m and n are the same asset and equal amount. The amount
// comparison is by value, so 0.10 and 0.1 of the same asset are equal.
func (m Money) Equal(n Money) bool {
	return m.asset == n.asset && m.amount.Equal(n.amount)
}

// IsZero reports whether the amount is exactly zero.
func (m Money) IsZero() bool { return m.amount.IsZero() }

// IsNegative reports whether the amount is strictly less than zero.
func (m Money) IsNegative() bool { return m.amount.IsNegative() }

// IsPositive reports whether the amount is strictly greater than zero.
func (m Money) IsPositive() bool { return m.amount.IsPositive() }

// Sign returns -1 if the amount is negative, 0 if zero, +1 if positive.
func (m Money) Sign() int { return m.amount.Sign() }
