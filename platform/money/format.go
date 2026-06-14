package money

import "strings"

// Position controls where the currency symbol is placed relative to the number.
type Position int

const (
	// SymbolNone omits the symbol entirely (rely on UseCode for the asset code).
	SymbolNone Position = iota
	// SymbolPrefix places the symbol before the number: "$1.23".
	SymbolPrefix
	// SymbolSuffix places the symbol after the number, separated by a space:
	// "1,23 €".
	SymbolSuffix
)

// NegativeStyle controls how negative amounts are rendered.
type NegativeStyle int

const (
	// NegMinus prefixes a minus sign to the whole token: "-$1.23".
	NegMinus NegativeStyle = iota
	// NegParens wraps the magnitude in parentheses with no minus: "($1.23)".
	NegParens
)

// ScaleMode controls the number of fractional digits shown.
type ScaleMode int

const (
	// ScaleAsset rounds (for display) to the asset's natural exponent.
	ScaleAsset ScaleMode = iota
	// ScaleFull shows the amount at its natural scale with no rounding — used
	// for crypto where 18 decimal places must round-trip exactly.
	ScaleFull
	// ScaleFixed rounds (for display) to FixedScale fractional digits.
	ScaleFixed
)

// FormatOptions is a display-only formatting recipe. Format may round the
// value FOR DISPLAY but never mutates the Money nor loses its underlying value.
type FormatOptions struct {
	DecimalSep string // "." or ","
	GroupSep   string // "," "." " " or "" (no grouping)
	GroupSize  int    // typically 3
	Symbol     string // explicit symbol; "" → symbolFor(asset) (presets), else none
	SymbolPos  Position
	UseCode    bool // append the asset code, e.g. "1.5 ETH"
	Negative   NegativeStyle
	Scale      ScaleMode
	FixedScale int32 // used when Scale == ScaleFixed
}

// Preset FormatOptions. The symbol is resolved per-asset via symbolFor unless
// an explicit Symbol is set, so the same preset renders "$" for USD, "¥" for
// JPY, and the code for crypto.
var (
	// US: "$1,234.50".
	US = FormatOptions{
		DecimalSep: ".", GroupSep: ",", GroupSize: 3,
		SymbolPos: SymbolPrefix, Negative: NegMinus, Scale: ScaleAsset,
	}
	// EU: "1.234,50 €".
	EU = FormatOptions{
		DecimalSep: ",", GroupSep: ".", GroupSize: 3,
		SymbolPos: SymbolSuffix, Negative: NegMinus, Scale: ScaleAsset,
	}
	// RU: "1 234,50 ₽".
	RU = FormatOptions{
		DecimalSep: ",", GroupSep: " ", GroupSize: 3,
		SymbolPos: SymbolSuffix, Negative: NegMinus, Scale: ScaleAsset,
	}
	// CryptoFmt: "1.5 ETH" (full natural scale, code suffix, no symbol). Named
	// CryptoFmt — not Crypto — because Crypto is already the asset Kind constant.
	CryptoFmt = FormatOptions{
		DecimalSep: ".", GroupSep: "", UseCode: true,
		SymbolPos: SymbolNone, Negative: NegMinus, Scale: ScaleFull,
	}
)

// symbolFor returns the display symbol for a fiat code, or "" for crypto and
// any asset without a well-known symbol (callers fall back to the code).
func symbolFor(asset string) string {
	switch asset {
	case "USD":
		return "$"
	case "EUR":
		return "€"
	case "GBP":
		return "£"
	case "JPY":
		return "¥"
	case "RUB":
		return "₽"
	default:
		return ""
	}
}

// Format renders m as a display string per o. It is display-only: m and its
// underlying value are never mutated. No float is used anywhere — the grouped
// string is built from the decimal's own string form.
func (m Money) Format(o FormatOptions) string {
	// 1. Determine the display scale and round a LOCAL copy (never m).
	d := Dec{m.amount}
	switch o.Scale {
	case ScaleAsset:
		a, _ := Lookup(m.asset)
		d = HalfEven.apply(d, a.Exponent)
	case ScaleFixed:
		d = HalfEven.apply(d, o.FixedScale)
	case ScaleFull:
		// keep natural scale, no rounding
	}

	// 2. Sign + magnitude string "<int>" or "<int>.<frac>".
	neg := strings.HasPrefix(d.d.String(), "-")
	mag := strings.TrimPrefix(d.d.String(), "-")
	intPart, fracPart, _ := strings.Cut(mag, ".")

	// For asset/fixed scale, pad trailing zeros to the target scale so e.g.
	// "$1,234.5" renders as "$1,234.50". ScaleFull keeps natural digits.
	var targetScale int32 = -1
	switch o.Scale {
	case ScaleAsset:
		a, _ := Lookup(m.asset)
		targetScale = a.Exponent
	case ScaleFixed:
		targetScale = o.FixedScale
	}
	if targetScale > 0 {
		if len(fracPart) < int(targetScale) {
			fracPart += strings.Repeat("0", int(targetScale)-len(fracPart))
		}
	}

	// 3. Group the integer part.
	intGrouped := groupDigits(intPart, o.GroupSep, o.GroupSize)

	// 4. Reassemble the numeric body.
	num := intGrouped
	if fracPart != "" {
		num += o.DecimalSep + fracPart
	}

	// 5. Symbol + code.
	sym := o.Symbol
	if sym == "" {
		sym = symbolFor(m.asset)
	}
	var b strings.Builder
	switch o.SymbolPos {
	case SymbolPrefix:
		b.WriteString(sym)
		b.WriteString(num)
	case SymbolSuffix:
		b.WriteString(num)
		if sym != "" {
			b.WriteByte(' ')
			b.WriteString(sym)
		}
	case SymbolNone:
		b.WriteString(num)
	}
	if o.UseCode {
		b.WriteByte(' ')
		b.WriteString(m.asset)
	}
	token := b.String()

	// 6. Negatives.
	if neg {
		switch o.Negative {
		case NegParens:
			return "(" + token + ")"
		case NegMinus:
			return "-" + token
		}
	}
	return token
}

// groupDigits inserts sep every size digits from the right of an integer-digit
// string. It is a no-op when sep is empty or size <= 0.
func groupDigits(intPart, sep string, size int) string {
	if sep == "" || size <= 0 || len(intPart) <= size {
		return intPart
	}
	n := len(intPart)
	first := n % size
	if first == 0 {
		first = size
	}
	var b strings.Builder
	b.WriteString(intPart[:first])
	for i := first; i < n; i += size {
		b.WriteString(sep)
		b.WriteString(intPart[i : i+size])
	}
	return b.String()
}
