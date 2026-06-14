package money

import "errors"

var (
	// ErrCurrencyMismatch is returned by binary operations on two Money values
	// whose asset codes differ.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrUnknownAsset is returned when an asset code is not in the registry.
	ErrUnknownAsset = errors.New("money: unknown asset")
	// ErrDivByZero is returned by DivRound when the divisor is zero.
	ErrDivByZero = errors.New("money: division by zero")

	// errNilAmount is returned by FromMinor when given a nil *big.Int.
	errNilAmount = errors.New("money: FromMinor: nil amount")
)
