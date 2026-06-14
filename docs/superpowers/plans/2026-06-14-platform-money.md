# `platform/money` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]`. **After EACH commit run `git log --oneline -1` and confirm the NEW sha — the pre-commit hook (build+gofumpt+golangci-lint, strict revive) WILL reject unused params / fmt.Errorf-without-args. Fix and re-commit until it lands. Never report a commit done without verifying.**

**Goal:** A precision-exact, immutable, general `money.Money` value type (fiat + crypto + FX) where the decimal library is fully encapsulated, every monetary operation is a method, and the absolute priority is never losing precision.

**Architecture:** `Money{amount decimal.Decimal /*unexported*/, asset string}` immutable; `shopspring/decimal` is an unexported impl detail behind `Money` and the `money.Dec` scalar wrapper. `Add/Sub/Mul` are exact and never round; rounding/division are explicit-only. Built-in ISO-4217 + crypto asset registry supplies per-asset exponents. Two-column NUMERIC+TEXT DB shape; decimal-as-string JSON/text/proto.

**Tech Stack:** Go 1.26, `github.com/shopspring/decimal`, pgx v5 (NUMERIC), testcontainers, buf/protobuf.

**Spec:** `docs/superpowers/specs/2026-06-14-platform-money-design.md`.

**Hard rules (every task upholds):** never lose precision (Add/Sub/Mul exact, no implicit rounding); no float in the API (no FromFloat); decimal lib never leaks (no exported method takes/returns `decimal.Decimal`); immutable (methods return new Money, receiver unchanged); same-asset arithmetic else `ErrCurrencyMismatch` (error, not panic on runtime values).

---

## Task 1: dependency + `dec.go` — the encapsulation seam

**Files:** modify `go.mod`/`go.sum`; create `platform/money/dec.go`, `platform/money/dec_test.go`.

- [ ] **Step 1: add the dep** — `go get github.com/shopspring/decimal@latest` (then `go mod tidy`). Confirm it lands in go.mod.

- [ ] **Step 2: failing test** (`dec_test.go`, package `money`)

```go
package money

import "testing"

func TestDec_NoFloatExactParse(t *testing.T) {
	d, err := DecFromString("0.0825")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := d.String(); got != "0.0825" {
		t.Fatalf("want 0.0825, got %s", got)
	}
	if _, err := DecFromString("abc"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestRoundingMode_MapsAllModes(t *testing.T) {
	// Every public mode must map to a concrete lib mode without panic.
	for _, m := range []RoundingMode{HalfEven, HalfUp, Down, Up, Ceil, Floor} {
		_ = m.apply(MustDec("1.005"), 2) // must not panic; returns a Dec
	}
}
```

- [ ] **Step 3: implement `dec.go`** — the ONLY file that imports `shopspring/decimal`; everything else uses `Dec`/`RoundingMode`.

```go
package money

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Dec is an exact decimal scalar (quantity, FX factor, percentage). It is the
// only way numbers enter Money operations — no float, no raw decimal.Decimal
// crosses the package boundary. Immutable.
type Dec struct{ d decimal.Decimal }

// DecFromString parses an exact decimal from its string form (no float path).
func DecFromString(s string) (Dec, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Dec{}, fmt.Errorf("money: parse decimal %q: %w", s, err)
	}
	return Dec{d}, nil
}

// MustDec is DecFromString for compile-time-known constants/tests; it panics on
// a bad literal (the time.Must idiom). Never call on runtime data.
func MustDec(s string) Dec {
	d, err := DecFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

// DecFromInt builds a Dec from an int64 (exact, no float).
func DecFromInt(n int64) Dec { return Dec{decimal.NewFromInt(n)} }

func (d Dec) String() string { return d.d.String() }
func (d Dec) IsZero() bool   { return d.d.IsZero() }

// RoundingMode is the package's rounding policy enum — does NOT leak the lib's
// type. HalfEven (banker's) is the documented default for money.
type RoundingMode int

const (
	HalfEven RoundingMode = iota // banker's — default
	HalfUp
	Down // toward zero (truncate)
	Up   // away from zero
	Ceil
	Floor
)

// apply rounds d to `scale` fractional digits under this mode, returning a Dec.
func (m RoundingMode) apply(d Dec, scale int32) Dec {
	switch m {
	case HalfUp:
		return Dec{d.d.RoundUp(scale)} // NOTE: confirm shopspring method names below
	case HalfEven:
		return Dec{d.d.RoundBank(scale)}
	case Down:
		return Dec{d.d.Truncate(scale)}
	case Up:
		return Dec{d.d.RoundCeil(scale)} // away-from-zero; adapt per lib
	case Ceil:
		return Dec{d.d.RoundCeil(scale)}
	case Floor:
		return Dec{d.d.RoundFloor(scale)}
	default:
		return Dec{d.d.RoundBank(scale)}
	}
}
```

ADAPT to the real shopspring API: confirm method names/signatures (`RoundBank(places int32)`, `RoundUp`, `RoundDown`, `RoundCeil`, `RoundFloor`, `Truncate(precision int32)`). shopspring's `RoundUp/RoundDown` are away-from-zero/toward-zero (NOT half-up). **Half-up** is `d.Round(places)` (shopspring `Round` is half-away-from-zero — verify; if "HalfUp" must be half-up specifically, use the method that matches and document). Make the mapping correct + add a test asserting each mode's result on a `.5` boundary (1.005 → HalfEven 1.00, HalfUp 1.01, etc.) — fix the mapping until the boundary test passes. This boundary test is the real spec for the modes.

- [ ] **Step 4: run** `go test ./platform/money/ -run 'TestDec|TestRoundingMode' -count=1` → PASS (after fixing mode mapping to real shopspring methods + a boundary-correctness test you add).
- [ ] **Step 5: commit** (`feat(money): decimal encapsulation seam — Dec + RoundingMode (hides shopspring)`). Verify SHA.

---

## Task 2: `asset.go` — registry (ISO-4217 + crypto)

**Files:** create `platform/money/asset.go`, `platform/money/asset_test.go`.

- [ ] **Step 1: failing test**

```go
package money

import "testing"

func TestAssetRegistry_BuiltinExponents(t *testing.T) {
	for code, want := range map[string]int32{"USD": 2, "EUR": 2, "JPY": 0, "BHD": 3, "BTC": 8, "ETH": 18, "USDT": 6} {
		a, ok := Lookup(code)
		if !ok {
			t.Fatalf("%s not registered", code)
		}
		if a.Exponent != want {
			t.Fatalf("%s exponent: want %d got %d", code, want, a.Exponent)
		}
	}
}

func TestAssetRegistry_RegisterCustomAndConflict(t *testing.T) {
	if err := Register(Asset{Code: "MYTOK", Exponent: 9, Kind: Crypto}); err != nil {
		t.Fatalf("register: %v", err)
	}
	a, ok := Lookup("MYTOK")
	if !ok || a.Exponent != 9 {
		t.Fatal("custom asset not registered")
	}
	// Re-registering the SAME definition is idempotent (ok); a CONFLICTING one errors.
	if err := Register(Asset{Code: "MYTOK", Exponent: 9, Kind: Crypto}); err != nil {
		t.Fatalf("idempotent re-register should be ok: %v", err)
	}
	if err := Register(Asset{Code: "MYTOK", Exponent: 2, Kind: Crypto}); err == nil {
		t.Fatal("conflicting re-register must error")
	}
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement `asset.go`**

```go
package money

import (
	"fmt"
	"sync"
)

type Kind int

const (
	Fiat Kind = iota
	Crypto
)

// Asset describes a currency/asset: its code and the exponent (number of decimal
// places in its smallest unit — USD 2, JPY 0, BHD 3, BTC 8, ETH 18).
type Asset struct {
	Code     string
	Exponent int32
	Kind     Kind
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Asset{}
)

func init() {
	for _, a := range builtinAssets() {
		registry[a.Code] = a
	}
}

// Register adds (or idempotently re-confirms) a custom asset. A conflicting
// redefinition of an existing code returns an error.
func Register(a Asset) error {
	if a.Code == "" {
		return fmt.Errorf("money: asset code empty")
	}
	if a.Exponent < 0 {
		return fmt.Errorf("money: asset %s negative exponent", a.Code)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := registry[a.Code]; ok && existing != a {
		return fmt.Errorf("money: asset %s already registered with different definition", a.Code)
	}
	registry[a.Code] = a
	return nil
}

// Lookup returns the asset for code.
func Lookup(code string) (Asset, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	a, ok := registry[code]
	return a, ok
}

// builtinAssets seeds all ISO-4217 fiat (correct exponents incl. JPY 0, BHD/KWD/
// JOD/OMR/TND 3) + common crypto. (Full ISO-4217 list — populate from the
// authoritative table; the test pins a representative subset.)
func builtinAssets() []Asset {
	return []Asset{
		{"USD", 2, Fiat}, {"EUR", 2, Fiat}, {"GBP", 2, Fiat}, {"JPY", 0, Fiat},
		{"KRW", 0, Fiat}, {"BHD", 3, Fiat}, {"KWD", 3, Fiat}, {"JOD", 3, Fiat},
		{"OMR", 3, Fiat}, {"TND", 3, Fiat}, /* … all ISO-4217 … */
		{"BTC", 8, Crypto}, {"ETH", 18, Crypto}, {"USDT", 6, Crypto},
		{"USDC", 6, Crypto}, {"SOL", 9, Crypto},
	}
}
```

The implementer SHOULD populate the FULL ISO-4217 set (use the official exponent table — most are 2; the 0- and 3-decimal exceptions are the ones that matter). Keep the list in `builtinAssets()`; the test pins the representative subset above. Note `existing != a` works because `Asset` is all-comparable fields.

- [ ] **Step 4: run → PASS.** `go test ./platform/money/ -run TestAssetRegistry -count=1`
- [ ] **Step 5: commit** (`feat(money): asset registry — ISO-4217 + crypto, extensible`). Verify SHA.

---

## Task 3: `money.go` — type, constructors, accessors, immutability

**Files:** create `platform/money/money.go`, `platform/money/money_test.go`.

- [ ] **Step 1: failing test**

```go
package money

import "testing"

func TestMoney_ConstructAndAccess(t *testing.T) {
	m, err := Parse("12.34", "USD")
	if err != nil { t.Fatal(err) }
	if m.Asset() != "USD" { t.Fatalf("asset: %s", m.Asset()) }
	if m.String() != "12.34 USD" { t.Fatalf("string: %s", m.String()) }
}

func TestMoney_UnknownAssetErrors(t *testing.T) {
	if _, err := Parse("1", "ZZZ"); err == nil {
		t.Fatal("unknown asset must error")
	}
}

func TestMoney_NoFloatAPI(t *testing.T) {
	// Compile-time guard: there must be NO FromFloat constructor. This is a
	// documentation test — assert the safe constructors exist and parse exactly.
	m, _ := Parse("0.1", "USD")
	n, _ := Parse("0.2", "USD")
	sum, _ := m.Add(n)
	if sum.String() != "0.3 USD" {
		t.Fatalf("0.1+0.2 must be exactly 0.3, got %s", sum.String())
	}
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement `money.go`**

```go
package money

import (
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"
)

// Money is an immutable monetary amount in a specific asset. All operations
// return a new Money; the receiver is never mutated (thread-safe to share).
type Money struct {
	amount decimal.Decimal
	asset  string
}

// Parse builds Money from a major-unit decimal string (e.g. "12.34") and an asset
// code. The asset must be registered. No float path. Full precision retained.
func Parse(s, asset string) (Money, error) {
	if _, ok := Lookup(asset); !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, asset)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Money{}, fmt.Errorf("money: parse %q: %w", s, err)
	}
	return Money{amount: d, asset: asset}, nil
}

// FromMajor is an alias of Parse for call-site clarity.
func FromMajor(s, asset string) (Money, error) { return Parse(s, asset) }

// FromMinor builds Money from an integer count of the asset's SMALLEST unit
// (cents, wei). E.g. FromMinor(big 1_000_000_000_000_000_000, "ETH") == 1 ETH.
func FromMinor(minor *big.Int, asset string) (Money, error) {
	a, ok := Lookup(asset)
	if !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, asset)
	}
	// value = minor × 10^-exponent  (exact)
	d := decimal.NewFromBigInt(minor, -a.Exponent)
	return Money{amount: d, asset: asset}, nil
}

// Zero is the zero amount of asset.
func Zero(asset string) (Money, error) { return Parse("0", asset) }

// MustParse is Parse for compile-time-known constants/tests; panics on bad input.
func MustParse(s, asset string) Money {
	m, err := Parse(s, asset)
	if err != nil {
		panic(err)
	}
	return m
}

// Asset returns the asset code.
func (m Money) Asset() string { return m.asset }

// String returns "<amount> <asset>", e.g. "12.34 USD".
func (m Money) String() string { return m.amount.String() + " " + m.asset }
```

Create `platform/money/errors.go` with the sentinel errors:
```go
package money

import "errors"

var (
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrUnknownAsset     = errors.New("money: unknown asset")
)
```
NB: `money.go` imports `shopspring/decimal` for the internal field type — that's the one OTHER allowed importer besides `dec.go` (the field type is unavoidable). No EXPORTED signature uses `decimal.Decimal`. Confirm `decimal.NewFromBigInt(value *big.Int, exp int32)` semantics (value × 10^exp) — so `-a.Exponent`.

- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): Money type + no-float constructors + accessors`). Verify SHA.

---

## Task 4: `arith.go` — Add/Sub/Mul/DivRound/Neg/Abs (exact, same-asset)

**Files:** create `platform/money/arith.go`, `platform/money/arith_test.go`.

- [ ] **Step 1: failing test** (precision is the spec)

```go
package money

import "testing"

func TestArith_AddSubExactSameAsset(t *testing.T) {
	a := MustParse("0.1", "USD")
	b := MustParse("0.2", "USD")
	sum, err := a.Add(b)
	if err != nil || sum.String() != "0.3 USD" {
		t.Fatalf("add: %v %s", err, sum.String())
	}
	diff, _ := b.Sub(a)
	if diff.String() != "0.1 USD" {
		t.Fatalf("sub: %s", diff.String())
	}
}

func TestArith_CrossAssetErrors(t *testing.T) {
	usd := MustParse("1", "USD")
	eth := MustParse("1", "ETH")
	if _, err := usd.Add(eth); err == nil {
		t.Fatal("cross-asset add must error")
	}
}

func TestArith_MulExactScaleGrows(t *testing.T) {
	price := MustParse("1.99", "USD")
	tax := MustDec("0.0825")
	got := price.Mul(tax) // exact, NO rounding
	if got.String() != "0.164175 USD" {
		t.Fatalf("mul must keep full precision, got %s", got.String())
	}
}

func TestArith_ImmutableReceiver(t *testing.T) {
	a := MustParse("5", "USD")
	_, _ = a.Add(MustParse("3", "USD"))
	if a.String() != "5 USD" {
		t.Fatal("Add mutated the receiver — Money must be immutable")
	}
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement `arith.go`**

```go
package money

import "fmt"

func (m Money) sameAsset(n Money) error {
	if m.asset != n.asset {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.asset, n.asset)
	}
	return nil
}

// Add returns m+n (exact, never rounds). Same-asset only.
func (m Money) Add(n Money) (Money, error) {
	if err := m.sameAsset(n); err != nil {
		return Money{}, err
	}
	return Money{amount: m.amount.Add(n.amount), asset: m.asset}, nil
}

// Sub returns m-n (exact). Same-asset only.
func (m Money) Sub(n Money) (Money, error) {
	if err := m.sameAsset(n); err != nil {
		return Money{}, err
	}
	return Money{amount: m.amount.Sub(n.amount), asset: m.asset}, nil
}

// Mul scales m by an exact decimal factor (quantity, rate). Exact: the result's
// scale grows; it is NOT rounded. Round explicitly at the boundary.
func (m Money) Mul(factor Dec) Money {
	return Money{amount: m.amount.Mul(factor.d), asset: m.asset}
}

// DivRound divides m by an exact divisor to `scale` fractional digits under mode.
// Division is the ONLY arithmetic that rounds, and only because it cannot be
// exact in general — the scale + mode are explicit. For dividing money AMONG
// recipients use Split/Allocate (which conserve units), not DivRound.
func (m Money) DivRound(divisor Dec, scale int32, mode RoundingMode) Money {
	q := Dec{m.amount.Div(divisor.d)}
	return Money{amount: mode.apply(q, scale).d, asset: m.asset}
}

// Neg returns -m. Abs returns |m|.
func (m Money) Neg() Money { return Money{amount: m.amount.Neg(), asset: m.asset} }
func (m Money) Abs() Money { return Money{amount: m.amount.Abs(), asset: m.asset} }
```

Confirm shopspring `Div` precision: shopspring `Div` uses `DivisionPrecision` (default 16) — so `Div` then `mode.apply(scale)` is correct (we round the high-precision quotient to the requested scale). Verify and, if needed, use `DivRound`/`QuoRem` from the lib for exactness control; the test below pins the contract.

- [ ] **Step 4: run → PASS** (incl. the immutability + 0.1+0.2 + scale-grows tests). Add a DivRound test: `MustParse("10","USD").DivRound(MustDec("3"),2,HalfEven)` → "3.33 USD".
- [ ] **Step 5: commit** (`feat(money): exact Add/Sub/Mul + explicit DivRound + Neg/Abs`). Verify SHA.

---

## Task 5: `alloc.go` — Split/Allocate (Fowler, conserve units)

**Files:** create `platform/money/alloc.go`, `platform/money/alloc_test.go`.

- [ ] **Step 1: failing test (conservation is the spec)**

```go
package money

import "testing"

func TestSplit_ConservesNoCentLost(t *testing.T) {
	ten := MustParse("10.00", "USD")
	parts, err := ten.Split(3)
	if err != nil { t.Fatal(err) }
	if len(parts) != 3 { t.Fatalf("parts: %d", len(parts)) }
	sum := MustParse("0", "USD")
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	if sum.String() != "10 USD" && sum.String() != "10.00 USD" {
		t.Fatalf("split must conserve total, got %s", sum.String())
	}
	// Fowler: earlier recipients get the extra cent. 10.00/3 → 3.34, 3.33, 3.33.
}

func TestAllocate_Weighted(t *testing.T) {
	pot := MustParse("0.05", "USD") // 5 cents among 3:2 weights
	parts, err := pot.Allocate(3, 2)
	if err != nil { t.Fatal(err) }
	sum := MustParse("0", "USD")
	for _, p := range parts { sum, _ = sum.Add(p) }
	if sum.String() != "0.05 USD" {
		t.Fatalf("allocate must conserve, got %s", sum.String())
	}
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement `alloc.go`** — Fowler allocate: work in the asset's smallest unit (integer minor units), integer-divide, distribute the remainder one minor-unit at a time to the first recipients, so the sum is exactly conserved.

```go
package money

import (
	"fmt"
	"math/big"
)

// Split divides m into n parts that sum EXACTLY to m, distributing any
// indivisible remainder one smallest-unit at a time to the earliest parts
// (Fowler's allocate). n must be > 0.
func (m Money) Split(n int) ([]Money, error) {
	weights := make([]int, n)
	for i := range weights {
		weights[i] = 1
	}
	return m.Allocate(weights...)
}

// Allocate divides m by integer ratios into len(ratios) parts that sum EXACTLY
// to m. Works in the asset's smallest unit so no value is lost or created.
func (m Money) Allocate(ratios ...int) ([]Money, error) {
	if len(ratios) == 0 {
		return nil, fmt.Errorf("money: allocate needs at least one ratio")
	}
	total := 0
	for _, r := range ratios {
		if r < 0 {
			return nil, fmt.Errorf("money: allocate negative ratio")
		}
		total += r
	}
	if total == 0 {
		return nil, fmt.Errorf("money: allocate ratios sum to zero")
	}
	a, _ := Lookup(m.asset) // asset known (Money construction validated it)
	// minorUnits = m × 10^exponent, as an integer (m must be at the asset's scale
	// or finer; round DOWN to the smallest unit first so allocation is integral).
	scaled := m.amount.Shift(a.Exponent)              // ×10^exp
	minor := scaled.BigInt()                          // floor to integer minor units
	remainder := new(big.Int).Set(minor)
	out := make([]Money, len(ratios))
	totalBig := big.NewInt(int64(total))
	for i, r := range ratios {
		// share = minor × r / total  (integer division)
		share := new(big.Int).Mul(minor, big.NewInt(int64(r)))
		share.Div(share, totalBig)
		out[i] = fromMinorUnits(share, a)
		remainder.Sub(remainder, share)
	}
	// Distribute the remainder one unit at a time to the earliest parts.
	one := big.NewInt(1)
	for i := 0; remainder.Sign() > 0; i = (i + 1) % len(out) {
		out[i] = out[i].addMinorUnit(a)
		remainder.Sub(remainder, one)
	}
	return out, nil
}
```

Implement the small helpers in `arith.go` or `alloc.go`: `fromMinorUnits(minor *big.Int, a Asset) Money` (= `Money{decimal.NewFromBigInt(minor, -a.Exponent), a.Code}`) and `addMinorUnit(a Asset) Money` (add `10^-exponent`). NB: `Shift`/`BigInt` truncates fractional-below-minor-unit — that is correct for allocation (you can only split down to the smallest unit). If `m` has sub-minor-unit precision, document that Split/Allocate operate at the asset's smallest unit (the natural divisible granularity). The test pins conservation at the smallest unit.

- [ ] **Step 4: run → PASS** (Split conserves, Allocate weighted conserves; add an 18dp-crypto split test: split 1 wei... actually split `MustParse("0.000000000000000010","ETH").Split(3)` conserves).
- [ ] **Step 5: commit** (`feat(money): Split/Allocate — Fowler, conserves smallest unit`). Verify SHA.

---

## Task 6: `round.go` — Round/RoundTo/Truncate/Fraction/Whole/Percent

**Files:** create `platform/money/round.go`, `platform/money/round_test.go`.

- [ ] **Step 1: failing tests** — Round to asset exponent (banker's default), RoundTo(scale,mode), Truncate, Fraction (sub-unit part), Whole (truncated to unit), Percent. Pin: `MustParse("1.005","USD").Round(HalfEven)` → "1.00 USD" (banker's: 1.00, not 1.01); `MustParse("1.015","USD").Round(HalfEven)` → "1.02"; `MustParse("3.459","USD").Whole()` → "3 USD" (or "3.00"?); `Fraction()` → "0.459 USD"; `MustParse("200","USD").Percent(MustDec("8.5"))` → "17 USD".

```go
func TestRound_BankersDefault(t *testing.T) {
	if got := MustParse("1.005", "USD").Round(HalfEven).String(); got != "1 USD" && got != "1.00 USD" {
		t.Fatalf("banker's 1.005→1.00, got %s", got)
	}
	if got := MustParse("1.015", "USD").Round(HalfEven).String(); got != "1.02 USD" {
		t.Fatalf("banker's 1.015→1.02, got %s", got)
	}
}
func TestPercent(t *testing.T) {
	if got := MustParse("200", "USD").Percent(MustDec("8.5")).String(); got != "17 USD" {
		t.Fatalf("8.5%% of 200 = 17, got %s", got)
	}
}
func TestFractionWhole(t *testing.T) {
	m := MustParse("3.459", "USD")
	if w := m.Whole().String(); w != "3 USD" { t.Fatalf("whole: %s", w) }
	if f := m.Fraction().String(); f != "0.459 USD" { t.Fatalf("fraction: %s", f) }
}
```

- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement `round.go`:**
```go
package money

// Round rounds to the asset's natural exponent under mode.
func (m Money) Round(mode RoundingMode) Money {
	a, _ := Lookup(m.asset)
	return m.RoundTo(a.Exponent, mode)
}
// RoundTo rounds to `scale` fractional digits under mode.
func (m Money) RoundTo(scale int32, mode RoundingMode) Money {
	return Money{amount: mode.apply(Dec{m.amount}, scale).d, asset: m.asset}
}
// Truncate cuts to `scale` digits toward zero (no rounding up).
func (m Money) Truncate(scale int32) Money {
	return Money{amount: m.amount.Truncate(scale), asset: m.asset}
}
// Whole truncates to the asset's unit (drops the fractional part).
func (m Money) Whole() Money { a, _ := Lookup(m.asset); return m.Truncate(a.Exponent).truncToUnit(a) }
// Fraction returns the sub-unit remainder: m - Whole(m).
func (m Money) Fraction() Money {
	w := m.Whole()
	f, _ := m.Sub(w)
	return f
}
// Percent returns p percent of m (= m × p/100), exact (no rounding).
func (m Money) Percent(p Dec) Money {
	return m.Mul(Dec{p.d.Div(hundred)})
}
```
ADAPT: define `hundred = decimal.NewFromInt(100)` (package var, internal). Fix `Whole` — "truncate to the asset's UNIT" means scale 0 in MAJOR units (drop everything after the decimal point of the major value), NOT the exponent. Re-read: `Whole()` of 3.459 USD = 3 USD (integer major units). So `Whole = m.Truncate(0)`. And `Fraction = m - Whole`. Simplify: `Whole() = Truncate(0)`; drop the `truncToUnit` helper. Make the tests (3.459 → whole 3, fraction 0.459) pass; that defines it.

- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): Round/RoundTo/Truncate/Whole/Fraction/Percent`). Verify SHA.

---

## Task 7: `compare.go` — Cmp/Equal/predicates

**Files:** create `platform/money/compare.go`, `platform/money/compare_test.go`.

- [ ] **Step 1: failing test** — `Cmp` same-asset returns -1/0/1 + errors cross-asset; `Equal`, `IsZero`, `IsNegative`, `IsPositive`, `Sign`.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement:**
```go
func (m Money) Cmp(n Money) (int, error) {
	if err := m.sameAsset(n); err != nil { return 0, err }
	return m.amount.Cmp(n.amount), nil
}
func (m Money) Equal(n Money) bool { return m.asset == n.asset && m.amount.Equal(n.amount) }
func (m Money) IsZero() bool     { return m.amount.IsZero() }
func (m Money) IsNegative() bool { return m.amount.IsNegative() }
func (m Money) IsPositive() bool { return m.amount.IsPositive() }
func (m Money) Sign() int        { return m.amount.Sign() }
```
- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): Cmp/Equal/Sign/predicates`). Verify SHA.

---

## Task 8: `convert.go` — Rate/Convert/RateProvider

**Files:** create `platform/money/convert.go`, `platform/money/convert_test.go`.

- [ ] **Step 1: failing test** — `Rate{From:"USD",To:"EUR",Factor:MustDec("0.92")}`; `MustParse("100","USD").Convert(rate)` → "92 EUR" (full precision, no IO); wrong-From → ErrCurrencyMismatch.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement:**
```go
package money

import "fmt"

// Rate is an exact FX factor from one asset to another. Supplied by the caller —
// money performs no IO.
type Rate struct {
	From, To string
	Factor   Dec
}

// RateProvider supplies rates (implemented by callers; money never calls it
// itself — defined here so services share one interface).
type RateProvider interface {
	Rate(from, to string) (Rate, error)
}

// Convert returns m converted to Rate.To at full precision (= amount × Factor in
// the target asset). Round explicitly at the boundary. m.asset must equal
// Rate.From; the target asset must be registered.
func (m Money) Convert(r Rate) (Money, error) {
	if m.asset != r.From {
		return Money{}, fmt.Errorf("%w: have %s, rate from %s", ErrCurrencyMismatch, m.asset, r.From)
	}
	if _, ok := Lookup(r.To); !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, r.To)
	}
	return Money{amount: m.amount.Mul(r.Factor.d), asset: r.To}, nil
}
```
- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): Rate + Convert (IO-free, full precision) + RateProvider iface`). Verify SHA.

---

## Task 9: `json.go` + `text.go` — serialization (decimal-as-string)

**Files:** create `platform/money/json.go`, `platform/money/text.go`, `platform/money/serialize_test.go`.

- [ ] **Step 1: failing test** — `{"amount":"123.45","asset":"USD"}` round-trip; 18dp crypto round-trip exact; a JSON-NUMBER amount (`{"amount":123.45,...}`) is REJECTED on unmarshal (no float ingress); `MarshalText` → `"123.45 USD"`, `UnmarshalText` round-trips.

```go
func TestJSON_StringRoundTripAndRejectNumber(t *testing.T) {
	m := MustParse("123.450000000000000001", "ETH")
	b, err := json.Marshal(m)
	if err != nil { t.Fatal(err) }
	var back Money
	if err := json.Unmarshal(b, &back); err != nil { t.Fatal(err) }
	if !back.Equal(m) { t.Fatalf("round-trip lost precision: %s vs %s", back, m) }
	if err := json.Unmarshal([]byte(`{"amount":123.45,"asset":"USD"}`), &Money{}); err == nil {
		t.Fatal("JSON-number amount must be rejected (float ingress)")
	}
}
```

- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** `json.go` (MarshalJSON → object with string amount; UnmarshalJSON into a `struct{Amount json.RawMessage; Asset string}` then require Amount be a JSON string — reject a bare number — then Parse) + `text.go` (MarshalText `"<amount> <asset>"`, UnmarshalText split on the last space, Parse). Both validate the asset via Parse.
- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** (`feat(money): JSON (string, reject number) + Text serialization`). Verify SHA.

---

## Task 10: `sql.go` — two-column NUMERIC + asset (DB integration)

**Files:** create `platform/money/sql.go`, `platform/money/sql_test.go`. Read how the repo scans NUMERIC via pgx (`platform/storage/pg`), and the `pgtest.NewDSN` helper.

- [ ] **Step 1: failing test (integration, Docker)** — create a table `t(amount NUMERIC NOT NULL, asset TEXT NOT NULL)`; insert via the money helpers; read back via `ScanRow`; assert exact round-trip for `"123.450000000000000001 ETH"` (18dp) and `"12.34 USD"`. Use `pgtest.NewDSN`.

```go
func TestSQL_NumericRoundTrip18dp(t *testing.T) {
	if testing.Short() { t.Skip("needs Docker") }
	pool := newTestPool(t) // create table t(amount numeric not null, asset text not null)
	ctx := context.Background()
	m := MustParse("123.450000000000000001", "ETH")
	_, err := pool.Writer().Exec(ctx, `insert into t(amount, asset) values ($1,$2)`, m.AmountValue(), m.Asset())
	require.NoError(t, err)
	var amountSrc money_pgNumeric // whatever type pgx scans NUMERIC into losslessly
	var assetSrc string
	require.NoError(t, pool.Reader().QueryRow(ctx, `select amount, asset from t`).Scan(&amountSrc, &assetSrc))
	got, err := ScanRow(amountSrc, assetSrc)
	require.NoError(t, err)
	require.True(t, got.Equal(m), "DB round-trip lost precision: %s", got)
}
```

- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement `sql.go`:**
  - `AmountValue() driver.Valuer` — returns the amount as a NUMERIC-compatible value (shopspring decimal implements `driver.Valuer`; OR convert to a `pgtype.Numeric`). Confirm shopspring's `Decimal` satisfies `driver.Valuer` (it does — emits the decimal string, which PG accepts into NUMERIC losslessly). So `AmountValue()` returns `m.amount`.
  - `Asset() string` already exists.
  - `ScanRow(amountSrc, assetSrc any) (Money, error)` — accept the scanned NUMERIC (a string / shopspring decimal / pgtype.Numeric) + the asset string, build Money via the decimal lib's scanner, validate asset registered. Provide a `*Money` `Scan` is NOT possible (two columns) — so `ScanRow` is the seam.
  - Document the canonical column DDL in the file header: `amount NUMERIC NOT NULL, asset TEXT NOT NULL`.
  Adapt `money_pgNumeric` in the test to the real lossless scan target (likely `decimal.Decimal` via shopspring's `sql.Scanner`, or `pgtype.Numeric`). Confirm pgx NUMERIC → shopspring decimal is lossless for 18dp (it is; both arbitrary precision).
- [ ] **Step 4: run → PASS** (`-p 1`).
- [ ] **Step 5: commit** (`feat(money): two-column NUMERIC+asset SQL helpers (lossless)`). Verify SHA.

---

## Task 11: `proto.go` + `money.proto` — decimal-as-string wire type

**Files:** create `proto/money/v1/money.proto` (+ `buf generate`), `platform/money/proto.go`, `platform/money/proto_test.go`. Read how the repo runs buf (`buf.gen.yaml`, `just gen`/`buf generate`, where generated Go lands, e.g. `gen/proto/...`).

- [ ] **Step 1: define the proto** — `message Money { string amount = 1; string asset = 2; }` in `proto/money/v1/money.proto` (package `money.v1`). Run `buf generate` (or `just gen`); commit the generated Go.
- [ ] **Step 2: failing test** — `ToProto(m)` → `*moneyv1.Money{Amount:"123.45...", Asset:"ETH"}`; `FromProto(pb)` round-trips exactly incl 18dp; `FromProto` with a number-ish/garbage amount errors.
- [ ] **Step 3: implement `proto.go`** — `ToProto(m Money) *moneyv1.Money` (amount = full-precision string, asset) + `FromProto(pb *moneyv1.Money) (Money, error)` (Parse). Document why `google.type.Money` is NOT used (int64+nanos caps 18dp crypto).
- [ ] **Step 4: run → PASS** + `buf lint` clean (CI gate).
- [ ] **Step 5: commit** (`feat(money): money.proto (decimal-as-string) + ToProto/FromProto`). Verify SHA.

> If the buf wiring proves heavy for this task, the FALLBACK is a plain Go struct `type Proto struct{ Amount, Asset string }` + helpers + a documented proto schema in the file — but PREFER the real `proto/money/v1` so services share the wire type. Decide by reading the repo's buf setup; do NOT leave it half-wired.

---

## Task 12: benches + final verification

**Files:** create `platform/money/bench_test.go`.

- [ ] **Step 1: benches (`-benchmem`)** — `BenchmarkAdd`, `Sub`, `Mul`, `Split`, `Round`, `Parse`, `MarshalJSON`, `ScanRow`. Report allocations (documents the immutability cost). Example:
```go
func BenchmarkAdd(b *testing.B) {
	x, y := MustParse("1234.56", "USD"), MustParse("78.90", "USD")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Add(y)
	}
}
```
- [ ] **Step 2: run benches** `go test ./platform/money/ -bench . -benchmem -run '^$' -count=1` → capture numbers (no assertion; documentation).
- [ ] **Step 3: full verification:**
  - `go build ./... && go vet ./platform/money/`
  - `go test ./platform/money/ -race -count=1 -p 1` → green (immutability/concurrency).
  - `golangci-lint run ./platform/money/...` → 0 issues.
  - coverage: `go test ./platform/money/ -cover -count=1` → report (target high; money is correctness-critical).
- [ ] **Step 4: commit** (`test(money): benchmarks + race/coverage verification`). Verify SHA.

---

## Self-review notes

- **Spec coverage:** Dec/RoundingMode encapsulation → T1; asset registry (ISO-4217+crypto) → T2; Money type + no-float constructors → T3; exact Add/Sub/Mul + explicit DivRound + Neg/Abs → T4; Split/Allocate conserve → T5; Round/RoundTo/Truncate/Whole/Fraction/Percent → T6; Cmp/Equal/predicates → T7; Rate/Convert/RateProvider → T8; JSON(string,reject-number)/Text → T9; two-column NUMERIC SQL → T10; proto decimal-as-string → T11; benches+race+coverage → T12.
- **Hard rules:** never-lose-precision (T4 0.1+0.2 exact, Mul scale-grows; T5 conserve; T9/T10 round-trip exact incl 18dp); no-float (no FromFloat in T3; T9 rejects JSON number); decimal-not-leaked (only dec.go + money.go internal field import shopspring; no exported signature uses decimal.Decimal — assert by review); immutable (T4 receiver-unchanged test, T12 -race); same-asset (T4/T7/T8 ErrCurrencyMismatch).
- **Type consistency:** `Dec`, `RoundingMode{HalfEven,HalfUp,Down,Up,Ceil,Floor}`, `Money`, `Asset{Code,Exponent,Kind}`, `Parse/FromMajor/FromMinor/Zero/MustParse`, `Add/Sub/Mul/DivRound/Neg/Abs/Split/Allocate/Round/RoundTo/Truncate/Whole/Fraction/Percent/Cmp/Equal/Convert`, `Rate{From,To,Factor}`, `AmountValue/Asset/ScanRow`, `ErrCurrencyMismatch/ErrUnknownAsset` — consistent across tasks.
- **OPEN flags for implementer:** (a) confirm exact shopspring method names for each RoundingMode (T1) — the .5-boundary test is the spec; (b) confirm shopspring `Decimal` satisfies `driver.Valuer`/`sql.Scanner` losslessly for NUMERIC (T10) — else use pgtype.Numeric; (c) buf wiring for money.proto (T11) — real proto preferred, documented-fallback allowed; (d) populate the FULL ISO-4217 table in builtinAssets (T2); (e) Whole()=Truncate(0), Fraction()=m-Whole() — pinned by the T6 test.
- **Encapsulation note:** `money.go` imports shopspring ONLY for the unexported struct field type; `dec.go` for the seam. NO other file should import shopspring, and NO exported function signature may name `decimal.Decimal`. The spec-reviewer must grep `decimal\.` across exported signatures.
