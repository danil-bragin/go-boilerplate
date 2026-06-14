# Design — `platform/money`: a precision-exact, immutable, general money type

**Date:** 2026-06-14
**Status:** Approved (brainstorm), pending implementation plan

## Goal

One `Money` value type covering EVERY monetary operation for a Go microservice
template, usable across fiat (0/2/3-decimal), crypto (18+ decimals), and FX —
with the **absolute, non-negotiable priority: never lose precision**. The
underlying decimal library is fully encapsulated (no leak); all use is through
methods. The type also owns its database representation and full serialization.

Backed by a deep-research report (2026-06-14): integer-coefficient+exponent
model, arbitrary-precision big.Int-backed decimal, NUMERIC storage, decimal-as-
string serialization, banker's rounding, Fowler allocation. Replaces the current
fiat-2dp-only `amount_cents BIGINT + currency TEXT`.

## Hard rules (every part upholds)

1. **Never lose precision.** `Add/Sub/Mul` are EXACT and never round; internal
   precision is unbounded. Rounding/division happen ONLY via explicit calls
   (`Round`, `RoundTo`, `DivRound`) with an explicit mode. Storage keeps full
   precision (NUMERIC, never truncated on write).
2. **No float, anywhere.** No `FromFloat` in the API. Construction only from
   string / integer / big.Int. (A float money value is already corrupted.)
3. **Encapsulation.** The decimal library (`shopspring/decimal`) is an unexported
   implementation detail. No method exposes or accepts a raw `decimal.Decimal`;
   scalars/rates/percentages use the package's own `money.Dec` wrapper. The lib
   is swappable without touching any caller.
4. **Immutable value type.** Methods return a NEW `Money`; the receiver is never
   mutated. Thread-safe without locks (a `Money` is safe to share across
   goroutines — matches the sharded/concurrent boilerplate).
5. **Same-asset safety.** Cross-asset arithmetic (USD+ETH) returns
   `ErrCurrencyMismatch`, never a silent wrong result and never a panic on a
   runtime value.

## Library + dependency

`github.com/shopspring/decimal` (add to go.mod): big.Int-backed coefficient +
exponent, immutable, no-float constructors, pgx-recommended for NUMERIC, max 2^31
fractional digits (covers 18dp crypto + 8dp FX). Hidden behind `money.Money` and
`money.Dec`. (apd is the documented alternative; encapsulation makes a later swap
local.)

## Type model

```go
// Money is an immutable monetary amount in a specific asset. Zero value is the
// zero-amount of the empty asset and is invalid for arithmetic until given an
// asset (construct via the constructors).
type Money struct {
	amount decimal.Decimal // unexported — full precision, never rounded implicitly
	asset  string          // ISO-4217 code or registered crypto/asset code
}

// Dec is an exact decimal scalar (quantity, FX factor, percentage) — the only
// way to pass numbers into Money operations without leaking decimal or floats.
type Dec struct{ d decimal.Decimal }
```

## Operation surface (all via methods)

**Construction (no float):** `Parse(s, asset) (Money, error)` (major-unit decimal
string, e.g. "12.34"), `FromMajor(s, asset)`, `FromMinor(n *big.Int, asset)`
(integer in the asset's smallest unit), `Zero(asset)`, plus `MustParse` /
`DecFromString` + `MustDec` for compile-time-known constants/tests (idiom:
`time.Must`). NO `FromFloat`.

**Arithmetic (exact, scale grows, never rounds):** `Add(Money) (Money, error)`,
`Sub(Money) (Money, error)` (same-asset → else `ErrCurrencyMismatch`);
`Mul(Dec) Money`; `Neg() Money`; `Abs() Money`.

**Division — explicit only (division is not exact in general):**
`DivRound(scale int, mode RoundingMode) Money`. No silent/lossy `Div`.

**Money-splitting (exact, conserves units — Fowler `allocate`):**
`Split(n int) ([]Money, error)` (equal split, remainder distributed unit-by-unit
so Σ == original — never lose/create a minor unit); `Allocate(ratios ...int)
([]Money, error)` (weighted).

**Percentage / fractional:** `Percent(Dec) Money` (= `Mul(p/100)`, exact);
`Fraction() Money` (the sub-unit remainder, i.e. amount minus its truncation to
the asset exponent); `Whole() Money` (truncated to the asset's unit);
`Truncate(scale int) Money`.

**Rounding (explicit):** `Round(mode) Money` (to the asset's exponent from the
registry; banker's/half-even is the default mode constant), `RoundTo(scale,
mode) Money`. `RoundingMode` is a package enum (`HalfEven` default, `HalfUp`,
`Down`, `Up`, `Ceil`, `Floor`) — does NOT leak the lib's mode type.

**Comparison / predicates:** `Cmp(Money) (int, error)` (same-asset), `Equal(Money)
bool`, `IsZero() bool`, `IsNegative() bool`, `IsPositive() bool`, `Sign() int`.

**Conversion:** `type Rate struct { From, To string; Factor Dec }`;
`Convert(Rate) (Money, error)` — requires `m.asset == Rate.From` (else
`ErrCurrencyMismatch`), returns `Money{amount × Factor, Rate.To}` at full
precision (caller rounds at the boundary). `money` never performs IO — the rate
is supplied (a `RateProvider` interface is defined for callers but money does not
call it).

**Accessors:** `Asset() string`, `String() string` ("12.34 USD"); minor/major
extraction for storage/display.

## Asset registry

```go
type Kind int // Fiat, Crypto
type Asset struct { Code string; Exponent int32; Kind Kind }
```
A package registry seeded with ALL ISO-4217 fiat (correct exponents: JPY 0, USD/
EUR 2, BHD/KWD/JOD 3) + common crypto (BTC 8, ETH 18, USDT/USDC 6, …). `Register(
Asset) error` adds custom tokens (idempotent; conflicting redefinition errors).
`Lookup(code) (Asset, bool)`. `Round`/store consult the registry for the
exponent — no hardcoded ×100. Unknown asset on a registry-needing op →
`ErrUnknownAsset`. The registry is the only mutable global; guarded for
concurrent reads (register at init, read after).

## Serialization (decimal-as-string everywhere)

- **JSON:** `MarshalJSON`/`UnmarshalJSON` → `{"amount":"123.45","asset":"USD"}`.
  amount is ALWAYS a JSON string, never a number (a JSON number is a float in
  most parsers → precision loss). Reject JSON-number amounts on unmarshal.
- **Text:** `MarshalText`/`UnmarshalText` → `"123.45 USD"` (map keys, env, headers).
- **Protobuf:** a `money.proto` message `{ string amount = 1; string asset = 2; }`
  + `ToProto`/`FromProto` helpers. `google.type.Money` (units int64 + nanos 10⁻⁹)
  is REJECTED — it caps fractions at 9 decimals and amounts at int64, so it cannot
  carry ETH wei (18dp). decimal-as-string survives any precision.
- **DB (two columns):** the canonical shape is `amount NUMERIC NOT NULL, asset
  TEXT NOT NULL`. `money` provides: `AmountValue() driver.Valuer` (full-precision
  NUMERIC via the lib's pgx/sql integration — pgx maps NUMERIC↔big losslessly),
  `Asset() string`, and `ScanRow(amountSrc, assetSrc any) (Money, error)` for
  repos. NEVER PG `money` type or float/double/real. SUM/aggregation is exact and
  per-asset (filter by asset column).

## Error model

Fallible operations return `error` (never panic on a runtime value):
`ErrCurrencyMismatch`, `ErrUnknownAsset`, parse/scan errors. Infallible ops
(`Neg`, `Abs`, `Mul`, `Round`, predicates) return only a value. `Must*`
constructors panic — but ONLY on compile-time-known inputs (constants/tests), the
standard-library idiom. This matches the "maximally safe" priority: no panic on
data-derived values.

## Files (focused, one responsibility each)

`platform/money/`: `money.go` (type, constructors, accessors), `dec.go` (Dec +
RoundingMode wrappers — the encapsulation seam), `arith.go` (Add/Sub/Mul/DivRound/
Neg/Abs), `alloc.go` (Split/Allocate — Fowler), `round.go` (Round/RoundTo/Truncate/
Fraction/Whole/Percent), `convert.go` (Rate/Convert/RateProvider), `asset.go`
(Asset/Kind/registry/ISO-4217+crypto seed), `compare.go` (Cmp/Equal/predicates),
`json.go`, `text.go`, `proto.go` (+ `money.proto`), `sql.go`. Each with a focused
`_test.go`; a `bench_test.go` for the hot ops.

## Testing (full coverage + benches)

- **Precision invariants:** `0.1 + 0.2 == 0.3` exact; a long multiply chain stays
  exact; ETH 18dp wei round-trips through every form; FX 8dp; sub-cent unit price.
- **Split/Allocate conserve:** `$10 / 3` parts sum to exactly $10 (no cent lost or
  created); weighted Allocate sums to the whole; works for 18dp crypto.
- **Rounding:** HalfEven vs HalfUp correctness at the .5 boundary; rounding only at
  explicit calls (Add/Mul never round — assert scale grows).
- **Invariants/safety:** cross-asset Add/Sub/Cmp/Convert → ErrCurrencyMismatch;
  unknown asset → ErrUnknownAsset; no `FromFloat` exists (API surface test);
  Money is immutable (a method call does not change the receiver).
- **Serialization round-trip:** JSON/text/proto/DB(NUMERIC) preserve the exact
  value, including 18dp crypto; JSON-number amount rejected on unmarshal.
- **Concurrency:** sharing one Money across goroutines is `-race` clean.
- **Benches (`-benchmem`):** Add/Sub/Mul/Split/Round/Parse/MarshalJSON/ScanRow,
  reporting allocations (documents the immutability cost).

## Out of scope (separate follow-up plan)

- Wiring the example services (orders/payments `amount_cents` → `Money`, the
  `amount NUMERIC + asset` migration). Done after `platform/money` lands + is
  verified, as its own plan.
- FX rate fetching / a concrete `RateProvider` implementation (money stays IO-free;
  the rate is supplied by the caller).
- A full double-entry ledger module (separate spec; `Money` is its building block).
