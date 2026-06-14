# Design — `platform/money` v2: error system + formatting + validators (+ crypto/large tests & benches)

**Date:** 2026-06-14
**Status:** Approved (brainstorm), pending implementation plan

## Goal

Extend the shipped `platform/money` package with: a thorough typed error system,
configurable money formatting (fiat + crypto), ready-made composable validation
rules, and expanded crypto / large-value test + benchmark coverage. The package
stays **dependency-light** (only `shopspring/decimal`) and keeps every existing
invariant (precision-exact, immutable, decimal-not-leaked, errors.Is-compatible).

## Hard rules

1. **Precision never lost.** Formatting may round FOR DISPLAY only (a new string);
   it never mutates or returns a lossy `Money`. Validators are read-only.
2. **Dependency-light core.** No new deps. The error/format/validate code lives in
   `platform/money` and imports only stdlib + the encapsulated `shopspring/decimal`.
   Adapters to `go-playground/validator` / `apperr` / `i18n` are explicitly OUT
   (separate follow-up), so the package stays portable.
3. **Backward-compatible errors.** `errors.Is(err, money.ErrCurrencyMismatch)`
   etc. keep working after the error-system refactor.
4. **decimal not leaked.** No new exported signature names `decimal.Decimal`.

## 1. Error system (foundation — format/validate depend on it)

Refactor `errors.go` from bare sentinels to a typed, code-bearing error that
carries structured context and is `errors.Is`/`errors.As`-friendly.

```go
type ErrorCode int

const (
	CodeCurrencyMismatch ErrorCode = iota + 1
	CodeUnknownAsset
	CodeDivByZero
	CodeInvalidAmount   // nil/empty/zero-value operand
	CodeParseFailed
	CodeOutOfRange      // Min/Max/Between
	CodeScaleExceeded   // MaxScale/ScaleAtMost
	CodeAssetNotAllowed // AllowedAssets
	CodeNotMultiple     // MultipleOf
	CodeInvalidRatio    // Allocate ratio
)

// Error is the package's structured error. Code drives programmatic handling;
// the string fields carry context (which assets/op/value). Wrapped holds an
// underlying cause (e.g. a shopspring parse error).
type Error struct {
	Code    ErrorCode
	Op      string // operation, e.g. "Add", "Parse", "Validate.Between"
	Asset   string // primary asset involved
	Asset2  string // second asset (mismatch)
	Detail  string // human detail / offending value
	wrapped error
}

func (e *Error) Error() string  // "money: <op>: <code-msg> (<context>)"
func (e *Error) Unwrap() error  // wrapped
func (e *Error) Is(target error) bool // true if target is *Error with the same Code
```

Sentinels become typed and match by Code:
```go
var (
	ErrCurrencyMismatch = &Error{Code: CodeCurrencyMismatch}
	ErrUnknownAsset     = &Error{Code: CodeUnknownAsset}
	ErrDivByZero        = &Error{Code: CodeDivByZero}
)
```
`Error.Is` compares `Code`, so `errors.Is(&Error{Code: CodeCurrencyMismatch, Asset:"USD", Asset2:"ETH"}, ErrCurrencyMismatch)` is true. Existing call sites that did `fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, a, b)` are migrated to construct `&Error{Code: CodeCurrencyMismatch, Op:"Add", Asset:a, Asset2:b}` (structured). An unexported constructor `newError(code, op string) *Error` + fluent `withAsset/withDetail/wrap` helpers keep call sites tidy. Callers: `var me *money.Error; if errors.As(err, &me) { switch me.Code { … } }`. A doc note states the canonical mapping to `apperr`/RFC-9457 lives in a future edge adapter, not in this package.

## 2. Formatting (display-only; fiat + crypto; no CLDR)

```go
type Position int      // SymbolNone, SymbolPrefix, SymbolSuffix
type NegativeStyle int // NegMinus ("-$1.23"), NegParens ("($1.23)")
type ScaleMode int     // ScaleAsset (asset exponent), ScaleFull, ScaleFixed
type FormatOptions struct {
	DecimalSep string // "." | ","
	GroupSep   string // "," | "." | " " | "" (no grouping)
	GroupSize  int    // 3
	Symbol     string // "$" | "€" | "" (empty → no symbol)
	SymbolPos  Position
	UseCode    bool   // append the asset code, e.g. "1.50 ETH"
	Negative   NegativeStyle
	Scale      ScaleMode
	FixedScale int32  // used when Scale == ScaleFixed
}

func (m Money) Format(o FormatOptions) string
```
- Grouping inserted from the right of the integer part; decimal/group separators configurable.
- `Scale=ScaleAsset` rounds to the asset's exponent (banker's) FOR DISPLAY (Money unchanged); `ScaleFull` shows all digits; `ScaleFixed` to `FixedScale`.
- `Negative=NegParens` wraps the magnitude in parens (accounting), suppressing the minus.
- Symbol placement via `SymbolPos`; `UseCode` appends the code (crypto default).
- Preset `FormatOptions` package vars: `US` (`$1,234.56`), `EU` (`1.234,56 €`), `RU` (`1 234,56 ₽`-style, code/symbol configurable), `Crypto` (full scale, code suffix, e.g. `1.5 ETH`). A small built-in symbol map (`$ € £ ¥ ₽`) backs the fiat presets; crypto presets use the code. `m.Format(money.US)`.
- Works for crypto/18dp (full scale) and 0/3-dp fiat. No locale CLDR dependency.

## 3. Validators (composable, dep-light)

```go
type Rule func(Money) *Error

// Validate runs rules in order, returning the FIRST failure as a *money.Error
// (nil if all pass). First-fail keeps the API simple; a ValidateAll aggregate is
// a future addition if needed.
func Validate(m Money, rules ...Rule) error
```
Built-in rules (each returns a `*Error` with the right Code on failure):
- `Positive()` / `NonNegative()` / `NonZero()` → CodeOutOfRange/CodeInvalidAmount.
- `Min(Money)`, `Max(Money)`, `Between(lo, hi Money)` → CodeOutOfRange; mismatched asset → CodeCurrencyMismatch.
- `MaxScale()` (no finer than the asset's exponent — reject sub-unit input) / `ScaleAtMost(n int32)` → CodeScaleExceeded.
- `AllowedAssets(codes ...string)` → CodeAssetNotAllowed.
- `MultipleOf(step Money)` (amount is an integer multiple of step) → CodeNotMultiple; mismatched asset → CodeCurrencyMismatch.
All are pure/read-only and dep-light.

## 4. Tests (emphasis: crypto / large values)

- **uint256-scale / large:** max-ish ETH and BTC amounts in wei/sats (70–78-digit
  integers) constructed via `FromMinor(big.Int)`; Add/Sub/Mul of huge values stay
  exact (no overflow — big.Int); dust (1 wei) arithmetic; an 18dp multiply chain
  exact; Split/Allocate of large + 18dp values conserve.
- **Format:** large grouped values, crypto full-scale (`1234567.890123456789 ETH`),
  accounting negatives, every preset, 0-dp (JPY) and 3-dp (BHD) assets, custom
  separators/symbol/position.
- **Validate:** every rule pass/fail incl. crypto + cross-asset + scale rules.
- **Error:** `errors.Is` (sentinel match by Code), `errors.As` extracting context
  (Asset/Asset2/Op), `Unwrap` of a wrapped parse error, the Code switch.

## 5. Benchmarks (extend `bench_test.go`, `-benchmem`)

Add: `Format` (a preset), `Validate` (several rules), `Convert`, `FromMinor`,
large/uint256-scale `Add` and `Mul`, `Allocate` of a large value. Report allocs.

## 6. Files

`platform/money/`: refactor `errors.go` → `error.go` (typed Error + codes +
sentinels + constructor helpers); new `format.go` + `format_test.go`; new
`validate.go` + `validate_test.go`; new `crypto_test.go` (large/uint256/18dp
coverage); extend `bench_test.go`. Migrate existing call sites in `arith.go`,
`money.go`, `convert.go`, `alloc.go` to construct `*Error` with context.

## Out of scope (separate follow-ups)

- Adapters: `money.Error` → `apperr`/RFC-9457; money rules → `go-playground/validator`
  custom validations; locale/CLDR i18n formatting (`golang.org/x/text`).
- Wiring `platform/money` into the example services (amount_cents → Money).
- A `ValidateAll` aggregate-error variant (add if a real need appears).
