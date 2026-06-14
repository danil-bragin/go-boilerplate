# ADR 0020 — Money representation (`platform/money`)

**Status:** Accepted
**Date:** 2026-06-14

## Context

The template stored amounts as `amount_cents BIGINT + currency TEXT` (see the
payments example). That model is fiat-2-decimal-only and breaks the moment the
template is used for anything else:

- **Crypto overflows int64.** 1 ETH is 10^18 wei; a `BIGINT` of wei overflows
  past ~9.2 ETH. Tokens with 18 decimals are routine.
- **FX needs sub-cent rates.** A USD→EUR rate is 6–8 decimals; "cents" cannot
  hold it, so conversions would round before they compute.
- **Mixed scale.** JPY has 0 decimals, USD/EUR 2, BHD/KWD 3, ETH 18, USDC 6. A
  single fixed scale is wrong for all but one of them.

A float is never an option: binary floating point cannot represent 0.10, so it
silently accumulates error — disqualifying for money. The goal is **one** Money
abstraction that covers fiat (0/2/3 dp), crypto (up to 18+ dp), and FX rates,
with no floating point anywhere and no loss of precision.

## Decision

A `platform/money` package with an immutable `Money` value type built on
arbitrary-precision decimals.

- **shopspring/decimal, fully encapsulated.** `Money{amount, asset}` where the
  decimal is unexported; `dec.go` is the only file importing shopspring and `Dec`
  is the only public scalar. The dependency never appears in an exported
  signature, so it can be swapped without breaking callers. Decimal (not
  `big.Int` minor-units) is chosen because exact `+ - ×` across mixed scales and
  the no-float construction are first-class, and arbitrary precision removes the
  int64 overflow ceiling entirely.

- **No float path.** Construction is from strings or integers only
  (`Parse`/`FromMajor`, `FromMinor`, `Dec` from string/int). There is no
  `FromFloat`.

- **Immutable.** Every operation returns a new Money; the receiver is never
  mutated. A Money is safe to share across goroutines.

- **Exact arithmetic, explicit rounding.** `Add`/`Sub`/`Mul` never round (scale
  grows). Division rounds, so it is explicit: `DivRound(divisor, scale, mode)`
  with guard digits so the mode rounds the TRUE quotient, not one already cut at
  the library's global division precision. Dividing money among recipients uses
  `Split`/`Allocate` (Fowler allocation at the smallest unit, sign-aware) which
  conserve every unit. Round ONCE at the boundary; `HalfEven` (banker's) is the
  default.

- **Same-asset invariant.** Binary operations on differing assets return
  `ErrCurrencyMismatch`; there is no implicit coercion. A typed `Error` with an
  `ErrorCode` drives programmatic handling.

- **Two-column Postgres storage: `amount NUMERIC + asset TEXT`.** NUMERIC is
  arbitrary precision/scale and lossless; `AmountValue` (a `driver.Valuer`) +
  `ScanRow` round-trip through both `database/sql` and pgx (proven by an
  integration test). NOT the PG `money` type (fixed 2dp, locale-dependent) and
  NOT float/double. `NullMoney` covers nullable columns. The per-asset exponent
  lives in a code registry (ISO-4217 fiat + crypto), not a table, so it is
  available without a query and identical across services.

- **String serialization across the wire.** JSON emits the amount as a STRING
  (a JSON number would be float-parsed by many decoders); the proto form is
  `money.v1.Money` with string fields. `google.type.Money` (int64 units + nanos)
  is rejected — nanos cap precision at 9 dp, too coarse for 18-dp crypto.

- **Untrusted input is magnitude-bounded.** The text-parse paths reject more than
  1000 significant digits or |exponent| > 256, so a short hostile string cannot
  expand into a multi-gigabyte value during rendering or exponent alignment (a
  DoS amplification). The bounds dwarf any real amount (256-bit ints are 78
  digits, crypto scale is 18). `FromMinor` is exempt — it already holds a
  materialized `big.Int`.

## Consequences

- The template has one money type for every asset class; the payments example
  adopts it (`amount NUMERIC + asset TEXT`) as the reference migration away from
  `amount_cents`.
- Callers never touch shopspring and cannot introduce a float; precision is
  structurally protected.
- A double-entry ledger is deliberately **out of scope** here. Amounts-on-rows
  (this design) suffice for an e-commerce/PSP flow. A service that owns balances
  and must guarantee debits/credits sum to zero should add an append-only
  double-entry ledger on top of Money — Money is the value type, not the
  accounting model.

## Alternatives considered

- **int64 minor-units ("store cents").** Simplest for fiat, but overflows for
  crypto and cannot hold FX rates. Rejected as not general.
- **big.Int minor-units + per-asset exponent.** No overflow, but every operation
  must track and align exponents by hand, and mixed-scale `+`/`×` and parsing
  become error-prone. Decimal gives the same range with exact arithmetic for
  free.
- **PG `money` / float columns.** Lossy and locale-dependent. Rejected.
