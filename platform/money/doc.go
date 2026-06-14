// Package money is a general-purpose, precision-exact monetary type for fiat,
// crypto, and FX in a single Money value. It is the boilerplate's canonical way
// to represent, store, and compute money. See docs/adr/0020-money-representation.md
// for the design rationale.
//
// # No float, ever
//
// Money wraps github.com/shopspring/decimal behind a narrow seam (Dec is the
// only public scalar; dec.go is the only file that imports shopspring).
// Construction is from strings or integers only — there is deliberately no
// float path, so binary floating-point error can never enter a money value.
//
// # Immutability
//
// Money is immutable: every operation returns a new value and the receiver is
// never mutated, so a Money is safe to share across goroutines.
//
// # Arithmetic discipline
//
// Add, Sub, and Mul are EXACT — they never round; the result's scale grows as
// needed. Division is the only operation that must round, so it is explicit:
// DivRound(divisor, scale, mode). To divide money AMONG recipients use
// Split/Allocate, which conserve every smallest unit (Fowler allocation) and so
// never lose or invent a cent. Keep full precision through a calculation and
// round ONCE at the settlement boundary with Round (banker's/HalfEven is the
// default, the least-biased choice for money).
//
// Same-asset is enforced: binary operations on differing assets return
// ErrCurrencyMismatch rather than silently coercing. Sum reduces many same-asset
// values; the comparators (Cmp, LessThan, GreaterThanOrEqual, Min, Max …) follow
// the same rule.
//
// # Untrusted input is bounded
//
// Amounts decoded from text (Parse, and thus JSON/proto, plus ScanRow) are
// bounded by a magnitude guard (<=1000 significant digits, |exponent| <= 256) so
// a tiny hostile string like "1e1000000000" cannot be expanded into a
// multi-gigabyte value — a denial-of-service amplification. The bounds are far
// wider than any real money (a 256-bit integer is 78 digits; the largest crypto
// scale is 18). FromMinor is exempt: it takes an already-materialized big.Int.
//
// # Storage
//
// The canonical Postgres shape is two columns — amount NUMERIC + asset TEXT —
// never the PG money type and never float/double. AmountValue (a driver.Valuer)
// and ScanRow move a Money losslessly through database/sql and pgx. NullMoney
// covers nullable columns and optional fields. Serialize across services as a
// decimal STRING (JSON object, or money.v1.Money proto), never a JSON number.
//
// # Assets
//
// The registry carries ISO-4217 fiat (with the correct minor-unit exponents,
// including the 0- and 3-decimal exceptions) and common crypto (BTC 8, ETH 18,
// USDT/USDC 6). Register adds custom assets. The asset's exponent defines its
// smallest unit, used by Round, Split/Allocate, Minor, and FromMinor.
package money
