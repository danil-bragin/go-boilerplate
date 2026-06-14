package money

import (
	"errors"
	"fmt"
)

// Rule is a composable money validation. It returns a *Error on failure and a
// bare nil on success. Validators are read-only — they never mutate the Money.
type Rule func(Money) *Error

// Validate runs rules in order and returns the FIRST failure (nil if all pass).
//
// On success it returns a BARE nil so the result compares == nil; it never
// wraps a typed-nil *Error in the error interface (the typed-nil trap).
func Validate(m Money, rules ...Rule) error {
	for _, r := range rules {
		if e := r(m); e != nil {
			return e // *Error → error; guaranteed non-nil
		}
	}
	return nil
}

// asErr coerces an error returned by a Money operation back to the package's
// typed *Error, so a Rule can propagate it without losing the concrete type.
// Cmp/sameAsset always return a *Error, so the assertion succeeds; the fallback
// re-wraps any non-*Error defensively.
func asErr(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return codeErr(CodeInvalidAmount, "Validate").wrap(err)
}

// Positive requires m > 0.
func Positive() Rule {
	return func(m Money) *Error {
		if m.Sign() > 0 {
			return nil
		}
		return codeErr(CodeOutOfRange, "Validate.Positive").withDetail("must be > 0")
	}
}

// NonNegative requires m >= 0.
func NonNegative() Rule {
	return func(m Money) *Error {
		if m.Sign() >= 0 {
			return nil
		}
		return codeErr(CodeOutOfRange, "Validate.NonNegative").withDetail("must be >= 0")
	}
}

// NonZero requires m != 0. A zero amount yields CodeInvalidAmount (a zero value
// is treated as a missing/invalid amount rather than an out-of-range bound).
func NonZero() Rule {
	return func(m Money) *Error {
		if !m.IsZero() {
			return nil
		}
		return codeErr(CodeInvalidAmount, "Validate.NonZero").withDetail("must be non-zero")
	}
}

// Min requires m >= lo. A cross-asset comparison propagates the
// CodeCurrencyMismatch from Cmp.
func Min(lo Money) Rule {
	return func(m Money) *Error {
		c, err := m.Cmp(lo)
		if err != nil {
			return asErr(err)
		}
		if c >= 0 {
			return nil
		}
		return codeErr(CodeOutOfRange, "Validate.Min").
			withAsset(m.asset).
			withDetail("must be >= " + lo.amount.String())
	}
}

// Max requires m <= hi. A cross-asset comparison propagates
// CodeCurrencyMismatch.
func Max(hi Money) Rule {
	return func(m Money) *Error {
		c, err := m.Cmp(hi)
		if err != nil {
			return asErr(err)
		}
		if c <= 0 {
			return nil
		}
		return codeErr(CodeOutOfRange, "Validate.Max").
			withAsset(m.asset).
			withDetail("must be <= " + hi.amount.String())
	}
}

// Between requires lo <= m <= hi (inclusive). A cross-asset comparison
// propagates CodeCurrencyMismatch.
func Between(lo, hi Money) Rule {
	return func(m Money) *Error {
		if e := Min(lo)(m); e != nil {
			e.Op = "Validate.Between"
			return e
		}
		if e := Max(hi)(m); e != nil {
			e.Op = "Validate.Between"
			return e
		}
		return nil
	}
}

// scaleOf returns the number of fractional digits in the amount: a negative
// decimal exponent is the count of fractional places, a non-negative exponent
// means no fractional digits. This reads the encapsulated decimal's Exponent()
// directly — no string parsing.
func scaleOf(m Money) int32 {
	if exp := m.amount.Exponent(); exp < 0 {
		return -exp
	}
	return 0
}

// MaxScale requires the amount's fractional-digit count not to exceed the
// asset's exponent (e.g. at most 2 places for USD, 18 for ETH).
func MaxScale() Rule {
	return func(m Money) *Error {
		a, ok := Lookup(m.asset)
		if !ok {
			return codeErr(CodeUnknownAsset, "Validate.MaxScale").withAsset(m.asset)
		}
		return scaleAtMost(m, a.Exponent, "Validate.MaxScale")
	}
}

// ScaleAtMost requires the amount's fractional-digit count not to exceed n.
func ScaleAtMost(n int32) Rule {
	return func(m Money) *Error {
		return scaleAtMost(m, n, "Validate.ScaleAtMost")
	}
}

func scaleAtMost(m Money, n int32, op string) *Error {
	if s := scaleOf(m); s > n {
		return codeErr(CodeScaleExceeded, op).
			withAsset(m.asset).
			withDetail(fmt.Sprintf("scale %d exceeds max %d", s, n))
	}
	return nil
}

// AllowedAssets requires m's asset to be one of codes.
func AllowedAssets(codes ...string) Rule {
	return func(m Money) *Error {
		for _, c := range codes {
			if m.asset == c {
				return nil
			}
		}
		return codeErr(CodeAssetNotAllowed, "Validate.AllowedAssets").withAsset(m.asset)
	}
}

// MultipleOf requires m to be an integer multiple of step (same asset). A zero
// step is an error. A cross-asset step propagates CodeCurrencyMismatch.
func MultipleOf(step Money) Rule {
	return func(m Money) *Error {
		if err := m.sameAsset(step); err != nil {
			return asErr(err)
		}
		if step.amount.IsZero() {
			return codeErr(CodeInvalidAmount, "Validate.MultipleOf").
				withAsset(m.asset).
				withDetail("step must be non-zero")
		}
		if !m.amount.Mod(step.amount).IsZero() {
			return codeErr(CodeNotMultiple, "Validate.MultipleOf").
				withAsset(m.asset).
				withDetail("must be a multiple of " + step.amount.String())
		}
		return nil
	}
}
