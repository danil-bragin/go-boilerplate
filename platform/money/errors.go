package money

import "errors"

var (
	// ErrCurrencyMismatch is returned by binary operations on two Money values
	// whose asset codes differ.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrUnknownAsset is returned when an asset code is not in the registry.
	ErrUnknownAsset = errors.New("money: unknown asset")
)
