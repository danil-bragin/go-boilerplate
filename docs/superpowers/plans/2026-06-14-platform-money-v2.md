# `platform/money` v2 — typed errors + formatting + validators — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]`. **After EACH commit run `git log --oneline -1` and confirm the NEW sha — the pre-commit hook (build+gofumpt+golangci-lint, strict revive/perfsprint) WILL reject no-arg `fmt.Errorf`, missing doc comments on exported, unused params. Fix and re-commit until it lands. Never report a commit done without verifying.**

**Goal:** Extend the shipped `platform/money` with a typed error system, configurable fiat+crypto formatting, composable validation rules, and expanded crypto/large-value tests + benches — keeping the package dependency-light, precision-exact, immutable, and `errors.Is`-compatible.

**Architecture:** A typed `money.Error{Code, Op, Asset, Asset2, Detail, wrapped}` with an `ErrorCode` enum replaces bare sentinels (sentinels become typed, match by Code, so `errors.Is` keeps working). `Format(FormatOptions)` is an own lightweight display formatter (presets US/EU/RU/Crypto; no CLDR). `Validate(m, rules...)` runs composable `Rule` funcs returning the first `*Error`. Only stdlib + the encapsulated `shopspring/decimal`.

**Tech Stack:** Go 1.26, `shopspring/decimal` (encapsulated), stdlib `errors`/`strings`/`math/big`.

**Spec:** `docs/superpowers/specs/2026-06-14-platform-money-v2-design.md`.

**Hard rules:** precision never lost (Format display-rounds a string only; validators read-only); dep-light (no new deps); `errors.Is(err, ErrCurrencyMismatch)` etc. keep working; no exported signature names `decimal.Decimal`.

---

## Task 1: typed error system (`error.go`) + migrate call sites

**Files:** rename/replace `platform/money/errors.go` → `platform/money/error.go`; create `platform/money/error_test.go`; modify `arith.go`, `money.go`, `convert.go`, `alloc.go`, `sql.go` (the sites that wrap the sentinels) to construct `*Error` with context. Leave dec.go/json.go/text.go/proto.go/asset.go generic `fmt.Errorf` as-is unless they wrap a sentinel (they mostly produce parse/format detail — wrap into `*Error{Code: CodeParseFailed/CodeInvalidAmount}` where it adds value; otherwise leave).

- [ ] **Step 1: failing test** (`error_test.go`, package `money`)

```go
package money

import (
	"errors"
	"testing"
)

func TestError_IsMatchesByCode(t *testing.T) {
	err := &Error{Code: CodeCurrencyMismatch, Op: "Add", Asset: "USD", Asset2: "ETH"}
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatal("errors.Is must match a sentinel by Code")
	}
	if errors.Is(err, ErrUnknownAsset) {
		t.Fatal("must not match a different code")
	}
}

func TestError_AsExtractsContext(t *testing.T) {
	_, err := MustParse("1", "USD").Add(MustParse("1", "ETH"))
	var me *Error
	if !errors.As(err, &me) {
		t.Fatal("errors.As must extract *money.Error")
	}
	if me.Code != CodeCurrencyMismatch || me.Asset != "USD" || me.Asset2 != "ETH" {
		t.Fatalf("context wrong: %+v", me)
	}
}

func TestError_UnwrapWrapped(t *testing.T) {
	_, err := Parse("not-a-number", "USD")
	var me *Error
	if !errors.As(err, &me) || me.Code != CodeParseFailed {
		t.Fatalf("parse error must be CodeParseFailed: %v", err)
	}
	if me.Unwrap() == nil {
		t.Fatal("parse error must wrap the underlying decimal error")
	}
}

func TestError_String(t *testing.T) {
	e := &Error{Code: CodeCurrencyMismatch, Op: "Add", Asset: "USD", Asset2: "ETH"}
	if e.Error() == "" {
		t.Fatal("Error() must be non-empty and human-readable")
	}
}
```

- [ ] **Step 2: run → FAIL** (no `Error` type / `Code*` / sentinels-as-Error).

- [ ] **Step 3: implement `error.go`** (replace errors.go)

```go
package money

import (
	"errors"
	"fmt"
)

// ErrorCode classifies a money error for programmatic handling.
type ErrorCode int

const (
	CodeCurrencyMismatch ErrorCode = iota + 1
	CodeUnknownAsset
	CodeDivByZero
	CodeInvalidAmount // nil / empty / zero-value operand
	CodeParseFailed
	CodeOutOfRange      // Min/Max/Between
	CodeScaleExceeded   // MaxScale/ScaleAtMost
	CodeAssetNotAllowed // AllowedAssets
	CodeNotMultiple     // MultipleOf
	CodeInvalidRatio    // Allocate ratio
)

func (c ErrorCode) String() string {
	switch c {
	case CodeCurrencyMismatch:
		return "currency mismatch"
	case CodeUnknownAsset:
		return "unknown asset"
	case CodeDivByZero:
		return "division by zero"
	case CodeInvalidAmount:
		return "invalid amount"
	case CodeParseFailed:
		return "parse failed"
	case CodeOutOfRange:
		return "out of range"
	case CodeScaleExceeded:
		return "scale exceeded"
	case CodeAssetNotAllowed:
		return "asset not allowed"
	case CodeNotMultiple:
		return "not a multiple"
	case CodeInvalidRatio:
		return "invalid ratio"
	default:
		return "unknown error"
	}
}

// Error is the package's structured error. Code drives handling; the string
// fields carry context; wrapped holds an underlying cause.
type Error struct {
	Code    ErrorCode
	Op      string
	Asset   string
	Asset2  string
	Detail  string
	wrapped error
}

func (e *Error) Error() string {
	var b []byte
	b = append(b, "money: "...)
	if e.Op != "" {
		b = append(b, e.Op...)
		b = append(b, ": "...)
	}
	b = append(b, e.Code.String()...)
	switch {
	case e.Asset != "" && e.Asset2 != "":
		b = append(b, fmt.Sprintf(" (%s vs %s)", e.Asset, e.Asset2)...)
	case e.Asset != "":
		b = append(b, fmt.Sprintf(" (%s)", e.Asset)...)
	}
	if e.Detail != "" {
		b = append(b, ": "...)
		b = append(b, e.Detail...)
	}
	if e.wrapped != nil {
		b = append(b, ": "...)
		b = append(b, e.wrapped.Error()...)
	}
	return string(b)
}

func (e *Error) Unwrap() error { return e.wrapped }

// Is matches another *Error by Code, so sentinels (Error with only Code set)
// work with errors.Is against a fully-populated Error.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// Sentinels (typed; match by Code via errors.Is).
var (
	ErrCurrencyMismatch = &Error{Code: CodeCurrencyMismatch}
	ErrUnknownAsset     = &Error{Code: CodeUnknownAsset}
	ErrDivByZero        = &Error{Code: CodeDivByZero}
)

// constructor helpers (unexported) — keep call sites tidy.
func codeErr(code ErrorCode, op string) *Error { return &Error{Code: code, Op: op} }
func (e *Error) withAssets(a, b string) *Error  { e.Asset, e.Asset2 = a, b; return e }
func (e *Error) withAsset(a string) *Error      { e.Asset = a; return e }
func (e *Error) withDetail(d string) *Error     { e.Detail = d; return e }
func (e *Error) wrap(err error) *Error          { e.wrapped = err; return e }
```

- [ ] **Step 4: migrate call sites** (construct `*Error` with context; keep behaviour):
  - `arith.go` sameAsset: `codeErr(CodeCurrencyMismatch,"").withAssets(m.asset,n.asset)`; empty-asset case → `codeErr(CodeCurrencyMismatch,"").withDetail("zero-value Money is not a valid operand")`. DivRound zero divisor → `codeErr(CodeDivByZero,"DivRound")` (already returns ErrDivByZero — can return the richer one or the sentinel; either errors.Is-matches).
  - `money.go` Parse unknown asset → `codeErr(CodeUnknownAsset,"Parse").withAsset(asset)`; parse failure → `codeErr(CodeParseFailed,"Parse").withDetail(s).wrap(err)`; FromMinor nil → `codeErr(CodeInvalidAmount,"FromMinor").withDetail("nil amount")`; FromMinor unknown asset → `codeErr(CodeUnknownAsset,"FromMinor").withAsset(asset)`.
  - `convert.go`: wrong From → `codeErr(CodeCurrencyMismatch,"Convert").withAssets(m.asset,r.From)`; unknown To → `codeErr(CodeUnknownAsset,"Convert").withAsset(r.To)`.
  - `sql.go` ScanRow: unknown asset → `codeErr(CodeUnknownAsset,"ScanRow").withAsset(assetSrc)`; scan failure → `codeErr(CodeParseFailed,"ScanRow").wrap(err)`.
  - `dec.go` DecFromString: keep wrapping but optionally `codeErr(CodeParseFailed,"DecFromString").withDetail(s).wrap(err)` (consistency). 
  - `alloc.go`: split n<=0 → `codeErr(CodeInvalidRatio,"Split").withDetail(...)`; ratio errors → `codeErr(CodeInvalidRatio,"Allocate").withDetail(...)`.
  - `json.go`/`text.go`: parse paths → `codeErr(CodeParseFailed, "UnmarshalJSON"/"UnmarshalText").wrap/withDetail`.
  - Remove the now-unused `errNilAmount` var.
  Keep `errors` import where still used; drop where not. Ensure NO bare-string `fmt.Errorf` remains that the perfsprint linter rejects (use `errors.New` or the typed Error).

- [ ] **Step 5: run → PASS** `go test ./platform/money/ -count=1` (all existing tests still pass — they use `errors.Is` against the sentinels, which still works; any test asserting an error *string* may need a tweak — update to assert `errors.Is`/Code instead).
- [ ] **Step 6: commit** (`feat(money): typed Error{Code,context} system — errors.Is/As, structured`). Verify SHA.

---

## Task 2: formatting (`format.go`)

**Files:** create `platform/money/format.go`, `platform/money/format_test.go`.

- [ ] **Step 1: failing test**

```go
package money

import "testing"

func TestFormat_Presets(t *testing.T) {
	cases := []struct{ amt, asset string; opt FormatOptions; want string }{
		{"1234.5", "USD", US, "$1,234.50"},
		{"1234.5", "EUR", EU, "1.234,50 €"},
		{"-1234.5", "USD", FormatOptions{Symbol: "$", SymbolPos: SymbolPrefix, GroupSep: ",", GroupSize: 3, DecimalSep: ".", Negative: NegParens, Scale: ScaleAsset}, "($1,234.50)"},
		{"1.5", "ETH", Crypto, "1.5 ETH"},
		{"123", "JPY", US, "¥123"}, // 0-dp asset
	}
	for _, c := range cases {
		got := MustParse(c.amt, c.asset).Format(c.opt)
		if got != c.want {
			t.Fatalf("%s %s: want %q got %q", c.amt, c.asset, c.want, got)
		}
	}
}

func TestFormat_CryptoFullScaleExact(t *testing.T) {
	m := MustParse("1234567.890123456789", "ETH")
	if got := m.Format(Crypto); got != "1234567.890123456789 ETH" {
		t.Fatalf("crypto full-scale: %s", got)
	}
}

func TestFormat_DisplayRoundDoesNotMutate(t *testing.T) {
	m := MustParse("1.005", "USD")
	_ = m.Format(US) // rounds to 2dp for display
	if m.String() != "1.005 USD" {
		t.Fatal("Format must not mutate the Money value")
	}
}
```

Confirm/adjust preset `want`s against the impl (e.g. `US` symbol for JPY — JPY's ¥; the preset uses the asset's symbol from the symbol map; if `US` is hard-coded `$`, then JPY under `US` would show `$` — decide: presets carry separators + negative style, and the SYMBOL comes from the asset's symbol map by default unless `Symbol` is set. Make presets symbol-by-asset so `¥123` is right; document. Adjust the test to the real design you implement — keep it self-consistent.)

- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement `format.go`** — `Position`, `NegativeStyle`, `ScaleMode` enums; `FormatOptions`; a `symbolFor(asset) string` backed by a small map (`USD $`, `EUR €`, `GBP £`, `JPY ¥`, `RUB ₽`; others/crypto → ""); preset vars `US`, `EU`, `RU`, `Crypto`. `Format`:
  1. choose scale (ScaleAsset → asset exponent via Lookup; ScaleFull → as-is; ScaleFixed → FixedScale), round the amount for display (banker's) into a string via the encapsulated decimal (do NOT mutate m).
  2. split sign/integer/fraction; group the integer part with GroupSep every GroupSize from the right; join with DecimalSep.
  3. apply symbol (explicit `Symbol` or `symbolFor(asset)` for presets) at SymbolPos; append code if UseCode.
  4. negatives: NegMinus prefixes "-"; NegParens wraps magnitude in "()".
  Keep it all string-building; no float.
- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): configurable Format (fiat+crypto) + US/EU/RU/Crypto presets`). Verify SHA.

---

## Task 3: validators (`validate.go`)

**Files:** create `platform/money/validate.go`, `platform/money/validate_test.go`.

- [ ] **Step 1: failing test**

```go
package money

import (
	"errors"
	"testing"
)

func TestValidate_Rules(t *testing.T) {
	usd := MustParse("10.00", "USD")
	if err := Validate(usd, Positive(), MaxScale(), AllowedAssets("USD", "EUR")); err != nil {
		t.Fatalf("valid money failed: %v", err)
	}
	// Positive fails on negative.
	if err := Validate(MustParse("-1", "USD"), Positive()); !errors.Is(err, ErrCurrencyMismatch) && err == nil {
		t.Fatal("negative must fail Positive")
	}
	mustCode(t, Validate(MustParse("-1", "USD"), Positive()), CodeOutOfRange)
	// Between.
	lo, hi := MustParse("5", "USD"), MustParse("20", "USD")
	if err := Validate(usd, Between(lo, hi)); err != nil {
		t.Fatalf("10 in [5,20]: %v", err)
	}
	mustCode(t, Validate(MustParse("100", "USD"), Between(lo, hi)), CodeOutOfRange)
	// MaxScale: sub-cent USD rejected.
	mustCode(t, Validate(MustParse("1.005", "USD"), MaxScale()), CodeScaleExceeded)
	// AllowedAssets.
	mustCode(t, Validate(MustParse("1", "JPY"), AllowedAssets("USD", "EUR")), CodeAssetNotAllowed)
	// MultipleOf.
	mustCode(t, Validate(MustParse("2.50", "USD"), MultipleOf(MustParse("1.00", "USD"))), CodeNotMultiple)
	// cross-asset range → currency mismatch
	mustCode(t, Validate(MustParse("1", "ETH"), Between(lo, hi)), CodeCurrencyMismatch)
}

func mustCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	var me *Error
	if !errors.As(err, &me) {
		t.Fatalf("want *Error code %v, got %v", want, err)
	}
	if me.Code != want {
		t.Fatalf("want code %v, got %v", want, me.Code)
	}
}
```

(Trim the messy first negative assertion — keep `mustCode(...Positive(), CodeOutOfRange)` as the canonical check.)

- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement `validate.go`** — `type Rule func(Money) *Error`; `func Validate(m Money, rules ...Rule) error` (first non-nil `*Error`, else nil — return `nil` typed as `error`, careful not to return a non-nil interface wrapping a nil `*Error`: return `error(nil)` explicitly when ok). Rules: `Positive/NonNegative/NonZero` (Sign/IsNegative checks → CodeOutOfRange or CodeInvalidAmount), `Min/Max/Between` (Cmp; mismatched asset → CodeCurrencyMismatch), `MaxScale` (compare the amount's fractional digits to the asset exponent → CodeScaleExceeded), `ScaleAtMost(n)`, `AllowedAssets(...)` (membership → CodeAssetNotAllowed), `MultipleOf(step)` (m / step is an integer; same-asset; → CodeNotMultiple). Each returns a `*Error` with Op like `"Validate.Between"`.
  NB the Validate nil-interface trap: `func Validate(...) error { for _, r := range rules { if e := r(m); e != nil { return e } }; return nil }` — returning the `*Error` directly when non-nil and a bare `nil` otherwise is fine (Go converts `*Error` to `error`; the trap is only if you return a typed-nil pointer — avoid `var e *Error; return e`).
- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): composable validators (Positive/Between/MaxScale/AllowedAssets/MultipleOf)`). Verify SHA.

---

## Task 4: crypto / large-value tests (`crypto_test.go`)

**Files:** create `platform/money/crypto_test.go`.

- [ ] **Step 1: write tests** (no new impl — exercises existing + v2 surface at scale):
  - **uint256-ish large:** build a ~70-78 digit wei amount via `FromMinor(bigFromString("115792089237316195423570985008687907853269984665640564039457584007913129639935"), "ETH")` (2^256-1) → Add/Sub/Mul (×2) stay exact (compare against an independently computed big.Int expectation); no overflow.
  - **dust:** `FromMinor(big 1, "ETH")` = 1 wei = "0.000000000000000001 ETH"; Add 1 wei + 1 wei = 2 wei.
  - **18dp chain:** multiply a crypto amount by "1.000000000000000001" several times — exact.
  - **Split large:** split a big wei amount by 7 — conserves (Σ == original).
  - **Format large/crypto:** `Format(Crypto)` of a 70-digit-ish amount round-trips the digits; grouped fiat of a large value.
  - **Validate crypto:** MaxScale on ETH (18dp ok), AllowedAssets("ETH"), Between with crypto bounds.
  Use a helper `bigFromString(s) *big.Int` (`new(big.Int).SetString(s,10)`).
- [ ] **Step 2: run → PASS** (`go test ./platform/money/ -run 'Crypto|Large|Dust' -count=1`).
- [ ] **Step 3: commit** (`test(money): crypto + large-value (uint256/dust/18dp) coverage`). Verify SHA.

---

## Task 5: benches + final verification

**Files:** extend `platform/money/bench_test.go`.

- [ ] **Step 1: add benches** (`-benchmem`): `BenchmarkFormat` (a preset), `BenchmarkValidate` (Positive+MaxScale+Between), `BenchmarkConvert`, `BenchmarkFromMinor`, `BenchmarkAddLarge` / `BenchmarkMulLarge` (uint256-scale operands), `BenchmarkAllocateLarge`. ReportAllocs.
- [ ] **Step 2: run benches** `go test ./platform/money/ -bench . -benchmem -run '^$' -count=1` → capture numbers.
- [ ] **Step 3: full verification:**
  - `go build ./... && go vet ./platform/money/`
  - `go test ./platform/money/ -race -count=1 -p 1` → green
  - `golangci-lint run ./platform/money/...` → 0 issues
  - coverage `go test ./platform/money/ -cover -count=1` → report (target ≥ 92%, keep/raise).
  - decimal-leak guard: `grep -rn "decimal\." platform/money/*.go | grep -v _test` → only unexported fields / internal use; NO exported signature names decimal.Decimal.
- [ ] **Step 4: commit** (`test(money): v2 benches + race/lint/coverage verification`). Verify SHA.
- [ ] **Step 5: push + CI** `git push origin main`; confirm CI green (build/lint/Test/buf — no proto change here, but the package compiles into the services that import nothing yet).

---

## Self-review notes

- **Spec coverage:** typed Error system + codes + Is/As + migrate sites → T1; Format + presets (fiat+crypto, display-only) → T2; composable validators → T3; crypto/large tests → T4; benches + verification → T5.
- **Hard rules:** precision (T2 Format display-rounds a string, T2 mutation test; validators read-only); dep-light (no new imports — only stdlib + decimal); errors.Is back-compat (T1 sentinel-by-Code test; existing tests still pass); decimal-not-leaked (T5 grep guard).
- **Type consistency:** `Error{Code,Op,Asset,Asset2,Detail,wrapped}`, `ErrorCode` + `Code*` consts, `ErrCurrencyMismatch/ErrUnknownAsset/ErrDivByZero` (typed), `FormatOptions{DecimalSep,GroupSep,GroupSize,Symbol,SymbolPos,UseCode,Negative,Scale,FixedScale}`, `Position{SymbolNone,SymbolPrefix,SymbolSuffix}`, `NegativeStyle{NegMinus,NegParens}`, `ScaleMode{ScaleAsset,ScaleFull,ScaleFixed}`, presets `US/EU/RU/Crypto`, `Rule`, `Validate`, `Positive/NonNegative/NonZero/Min/Max/Between/MaxScale/ScaleAtMost/AllowedAssets/MultipleOf` — consistent across tasks.
- **OPEN flags for implementer:** (a) preset symbol behaviour — presets pull the symbol from `symbolFor(asset)` so JPY under `US` shows `¥`; make the format_test wants self-consistent with the impl; (b) Validate must not return a typed-nil `*Error` as a non-nil `error` (return bare `nil`); (c) migrating error sites must keep every existing test green (those assert `errors.Is`, which still works) — if any test asserts an error *string*, switch it to `errors.Is`/Code; (d) `MaxScale` needs the amount's true fractional-digit count vs the asset exponent — use the decimal's exponent/scale accessor (confirm shopspring `Exponent()`), not the string.
