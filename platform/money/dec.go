package money

import (
	"github.com/shopspring/decimal"
)

// Dec is an exact decimal scalar. The wrapped shopspring value is unexported so
// the dependency stays hidden behind this package's API.
type Dec struct {
	d decimal.Decimal
}

// DecFromString parses an exact decimal from its string representation.
// Trailing zeroes are preserved by the underlying library at parse time.
func DecFromString(s string) (Dec, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Dec{}, codeErr(CodeParseFailed, "DecFromString").withDetail(s).wrap(err)
	}
	return Dec{d: d}, nil
}

// MustDec parses an exact decimal and panics on a bad literal. It is intended
// for compile-time-constant literals (the time.Must idiom), never for input.
func MustDec(s string) Dec {
	d, err := DecFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

// DecFromInt builds an exact decimal from an int64.
func DecFromInt(n int64) Dec {
	return Dec{d: decimal.NewFromInt(n)}
}

// String returns the canonical decimal string. Trailing zeroes are trimmed.
func (x Dec) String() string {
	return x.d.String()
}

// IsZero reports whether the value is exactly zero.
func (x Dec) IsZero() bool {
	return x.d.IsZero()
}

// RoundingMode selects how a Dec is rounded to a given scale. The zero value is
// HalfEven (banker's rounding), the safe default for money.
type RoundingMode int

const (
	// HalfEven rounds half to the nearest even digit (banker's rounding).
	// It is the default and minimizes cumulative bias across many roundings.
	HalfEven RoundingMode = iota
	// HalfUp rounds half away from zero (1.005 -> 1.01, -1.005 -> -1.01).
	HalfUp
	// Down truncates toward zero (1.5 -> 1, -1.5 -> -1), no rounding of the
	// dropped digits.
	Down
	// Up rounds away from zero at the given scale (1.1 -> 2, -1.1 -> -2).
	Up
	// Ceil rounds toward positive infinity.
	Ceil
	// Floor rounds toward negative infinity.
	Floor
)

// apply rounds d to the given number of decimal places using the mode's
// semantics. Each mode maps to a specific shopspring method; see the constant
// docs for exact behaviour. An unknown mode falls back to HalfEven.
func (m RoundingMode) apply(d Dec, scale int32) Dec {
	switch m {
	case HalfEven:
		return Dec{d: d.d.RoundBank(scale)}
	case HalfUp:
		return Dec{d: d.d.Round(scale)}
	case Down:
		return Dec{d: d.d.Truncate(scale)}
	case Up:
		return Dec{d: d.d.RoundUp(scale)}
	case Ceil:
		return Dec{d: d.d.RoundCeil(scale)}
	case Floor:
		return Dec{d: d.d.RoundFloor(scale)}
	default:
		return Dec{d: d.d.RoundBank(scale)}
	}
}
